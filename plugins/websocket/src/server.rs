use std::{
    collections::HashMap,
    f32::consts::E,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        RwLock,
    },
};

use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use tauri::{ipc::Channel, Manager, Runtime};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{oneshot, Mutex},
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{protocol::CloseFrame as ProtocolCloseFrame, Message},
    WebSocketStream,
};

use crate::{
    message::{CloseFrame, WebSocketMessage},
    types::{Error, Id},
};

#[derive(Default)]
/// Handles all active WebSocket servers. Can be used to stop a server by its ID.
pub struct ServerManager {
    servers: Mutex<HashMap<Id, Option<oneshot::Sender<()>>>>,
    next_id: AtomicU32,
}

impl ServerManager {
    pub fn new() -> Self {
        Self {
            servers: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(0),
        }
    }

    pub async fn add_server(&self) -> (Id, oneshot::Receiver<()>) {
        let (send, receive) = oneshot::channel();

        let id = self.next_id.fetch_add(1, Relaxed);

        self.servers.lock().await.insert(id, Some(send));

        (id, receive)
    }

    pub async fn remove_server(&self, id: Id) -> Result<(), Error> {
        let mut server_manager = self.servers.lock().await;

        if let Some(server) = server_manager.get_mut(&id) {
            if let Some(kill) = server.take() {
                kill.send(()).map_err(|_| Error::FailedToStopServer(id))?;
            } else {
                return Err(Error::FailedToStopServer(id));
            }
        } else {
            return Err(Error::ServerNotFound(id));
        }

        server_manager.remove(&id);

        Ok(())
    }
}

pub struct ServerConnectionManager {
    writes: Mutex<HashMap<Id, SplitSink<WebSocketStream<TcpStream>, Message>>>,
    subscribers: Mutex<HashMap<Id, Vec<Channel<serde_json::Value>>>>,
    next_id: AtomicU32,
}

impl ServerConnectionManager {
    pub fn new() -> Self {
        Self {
            writes: Mutex::new(HashMap::new()),
            subscribers: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(0),
        }
    }

    pub async fn add_connection(
        &self,
        write: SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> Id {
        let id = self.next_id.fetch_add(1, Relaxed); // TODO: Use a proper ID generation strategy
        self.writes.lock().await.insert(id, write);
        self.subscribers.lock().await.insert(id, Vec::new());
        id
    }

    pub async fn remove_connection(&self, id: Id) -> Result<(), Error> {
        if self.writes.lock().await.remove(&id).is_none() {
            return Err(Error::ConnectionNotFound(id));
        }
        Ok(())
    }

    pub async fn send_message(&self, id: Id, message: Message) -> Result<(), Error> {
        if let Some(write) = self.writes.lock().await.get_mut(&id) {
            write.send(message).await?;
            Ok(())
        } else {
            Err(Error::ConnectionNotFound(id))
        }
    }

    pub async fn subscribe(
        &self,
        id: Id,
        channel: Channel<serde_json::Value>,
    ) -> Result<(), Error> {
        // Add the channel to the subscribers list
        if let Some(subscribers) = self.subscribers.lock().await.get_mut(&id) {
            subscribers.push(channel);
            Ok(())
        } else {
            Err(Error::ConnectionNotFound(id))
        }
    }

    pub async fn send_value_to_subscribers(
        &self,
        id: Id,
        value: serde_json::Value,
    ) -> Result<(), Error> {
        if let Some(subscribers) = self.subscribers.lock().await.get_mut(&id) {
            subscribers.retain(|subscriber| subscriber.send(value.clone()).is_ok());
            Ok(())
        } else {
            Err(Error::ConnectionNotFound(id))
        }
    }

    pub async fn send_message_to_subscribers(
        &self,
        id: Id,
        message: WebSocketMessage,
    ) -> Result<(), Error> {
        self.send_value_to_subscribers(id, serde_json::to_value(message).unwrap())
            .await
    }

    pub async fn send_error_to_subscribers(&self, id: Id, error: Error) -> Result<(), Error> {
        self.send_message_to_subscribers(id, WebSocketMessage::Error(error.to_string()))
            .await
    }
}

fn convert_message_to_value(message: Message) -> serde_json::Value {
    match message {
        Message::Text(t) => serde_json::to_value(WebSocketMessage::Text(t.to_string())).unwrap(),
        Message::Binary(t) => serde_json::to_value(WebSocketMessage::Binary(t.to_vec())).unwrap(),
        Message::Ping(t) => serde_json::to_value(WebSocketMessage::Ping(t.to_vec())).unwrap(),
        Message::Pong(t) => serde_json::to_value(WebSocketMessage::Pong(t.to_vec())).unwrap(),
        Message::Close(t) => serde_json::to_value(WebSocketMessage::Close(t.map(|v| CloseFrame {
            code: v.code.into(),
            reason: v.reason.to_string(),
        })))
        .unwrap(),
        Message::Frame(_) => serde_json::Value::Null, // This value can't be recieved.
    }
}

fn convert_message_to_websocket_message(message: Message) -> Result<WebSocketMessage, Error> {
    match message {
        Message::Text(t) => Ok(WebSocketMessage::Text(t.to_string())),
        Message::Binary(t) => Ok(WebSocketMessage::Binary(t.to_vec())),
        Message::Ping(t) => Ok(WebSocketMessage::Ping(t.to_vec())),
        Message::Pong(t) => Ok(WebSocketMessage::Pong(t.to_vec())),
        Message::Close(t) => Ok(WebSocketMessage::Close(t.map(|v| CloseFrame {
            code: v.code.into(),
            reason: v.reason.to_string(),
        }))),
        Message::Frame(_) => Err(Error::InvalidMessageType),
    }
}

fn convert_websocket_message_to_message(message: WebSocketMessage) -> Result<Message, Error> {
    match message {
        WebSocketMessage::Text(t) => Ok(Message::Text(t.into())),
        WebSocketMessage::Binary(t) => Ok(Message::Binary(t.into())),
        WebSocketMessage::Ping(t) => Ok(Message::Ping(t.into())),
        WebSocketMessage::Pong(t) => Ok(Message::Pong(t.into())),
        WebSocketMessage::Close(t) => Ok(Message::Close(t.map(|v| ProtocolCloseFrame {
            code: v.code.into(),
            reason: v.reason.to_string().into(),
        }))),
        WebSocketMessage::Error(e) => Err(Error::InvalidMessageType),
    }
}

async fn handle_connection<R: Runtime>(
    socket: TcpStream,
    on_connection: Channel<u32>,
    window: tauri::Window<R>,
) -> Result<(), Error> {
    let ws_stream = accept_async(socket).await?;
    let (write, mut read) = ws_stream.split();

    let manager = window.state::<ServerConnectionManager>();
    let id = manager.add_connection(write).await;

    on_connection
        .send(id)
        .map_err(|e| Error::ConnectionClosed(id, e))?;

    tauri::async_runtime::spawn(async move {
        loop {
            match read.next().await {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                    break;
                }
                Some(Ok(message)) => {
                    let connections = window.state::<ServerConnectionManager>();
                    let message = convert_message_to_websocket_message(message)
                        .unwrap_or(WebSocketMessage::Error("Invalid message".to_string()));

                    if let Err(e) = connections.send_message_to_subscribers(id, message).await {
                        eprintln!("Error sending message to subscribers: {}", e);
                        break;
                    }
                }
            }
        }

        let manager = window.state::<ServerConnectionManager>();

        if let Err(e) = manager.remove_connection(id).await {
            eprintln!("Error removing connection {}: {}", id, e);
        }
    });

    Ok(())
}

#[tauri::command]
pub async fn new_server<R: Runtime>(
    port: u16,
    window: tauri::Window<R>,
    on_connection: Channel<u32>,
    server_manager: tauri::State<'_, ServerManager>,
) -> Result<(), String> {
    let listen = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port)))
        .await
        .map_err(|e| format!("Failed to bind to port {}: {}", port, e))?;

    let (id, mut kill_req) = server_manager.add_server().await;

    tauri::async_runtime::spawn(async move {
        loop {
            let window = window.clone();
            let on_connection = on_connection.clone();
            tokio::select! {
                Ok((stream, addr)) = listen.accept() => {
                    if let Err(e) = handle_connection( stream, on_connection.clone(), window).await {
                        eprintln!("Error handling connection from {}: {}", addr, e);
                    }
                }
                Ok(()) = &mut kill_req => {
                    if let Err(e) = window.state::<ServerManager>().remove_server(id).await {
                        eprintln!("Error stopping server {}: {}", id, e);
                    }
                    break;
                }
            }
        }
    });

    Ok(())
}

#[tauri::command]
pub async fn stop_server(
    id: Id,
    server_manager: tauri::State<'_, ServerManager>,
) -> Result<(), String> {
    server_manager
        .remove_server(id)
        .await
        .map_err(|e| format!("Failed to stop server {}: {}", id, e))?;

    Ok(())
}

#[tauri::command]
pub async fn subscribe_server(
    id: Id,
    on_message: Channel<serde_json::Value>,
    server_manager: tauri::State<'_, ServerConnectionManager>,
) -> Result<(), String> {
    server_manager
        .subscribe(id, on_message)
        .await
        .map_err(|e| format!("Failed to subscribe to server {}: {}", id, e))?;

    Ok(())
}

#[tauri::command]
pub async fn send_server_conn(
    id: Id,
    message: WebSocketMessage,
    server_manager: tauri::State<'_, ServerConnectionManager>,
) -> Result<(), String> {
    server_manager
        .send_message(id, convert_websocket_message_to_message(message).unwrap())
        .await
        .map_err(|e| format!("Failed to send message to server {}: {}", id, e))?;

    Ok(())
}
