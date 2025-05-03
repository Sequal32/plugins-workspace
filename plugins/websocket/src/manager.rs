use futures_util::{stream::SplitSink, SinkExt};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU32, Ordering::Relaxed},
};
use tauri::ipc::Channel;
use tokio::{
    net::TcpStream,
    sync::{oneshot, Mutex},
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

use crate::{
    message::WebSocketMessage,
    types::{Error, Id},
};

#[derive(Default)]
/// Handles all active WebSocket servers. Can be used to stop a server by its ID.
pub struct ServerManager {
    servers: Mutex<HashMap<Id, Option<oneshot::Sender<()>>>>,
    next_id: AtomicU32,
}

impl ServerManager {
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
        self.subscribers.lock().await.remove(&id);
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

impl Default for ServerConnectionManager {
    fn default() -> Self {
        Self {
            writes: Mutex::new(HashMap::new()),
            subscribers: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(0),
        }
    }
}
