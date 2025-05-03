use futures_util::{stream::SplitStream, StreamExt};
use log::error;
use tauri::{ipc::Channel, Manager, Runtime};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot::Receiver,
};
use tokio_tungstenite::accept_async;

use crate::{
    manager::{ServerConnectionManager, ServerManager},
    types::Error,
};

/// Starts handling the WebSocket connection.
async fn accept_connection<R: Runtime>(
    socket: TcpStream,
    on_connection: Channel<u32>,
    window: tauri::Window<R>,
) -> Result<(), Error> {
    let ws_stream = accept_async(socket).await?;
    let (write, mut read) = ws_stream.split();

    let manager = window.state::<ServerConnectionManager>();
    let id = manager.add_connection(write).await;

    if let Err(e) = on_connection.send(id) {
        error!("Failed to send connection ID {}: {}", id, e);
        return Err(Error::ConnectionClosed(id, e.to_string()));
    }

    let handler = ConnectionHandler::new(id, window.clone());

    tauri::async_runtime::spawn(async move {
        if let Err(e) = handler.process_messages(&mut read).await {
            error!("Error handling connection {}: {}", id, e);
        }

        if let Err(e) = handler.on_shutdown().await {
            error!("Error shutting down connection {}: {}", id, e);
        }
    });

    Ok(())
}

pub async fn handle_server<R: Runtime>(
    listen: TcpListener,
    id: u32,
    mut kill_req: Receiver<()>,
    window: tauri::Window<R>,
    on_connection: Channel<u32>,
) {
    loop {
        tokio::select! {
            Ok((stream, addr)) = listen.accept() => {
                if let Err(e) = accept_connection(stream, on_connection.clone(), window.clone()).await {
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
}

/// Helper struct to manager WebSocket connections.
struct ConnectionHandler<R: Runtime> {
    id: u32,
    window: tauri::Window<R>,
}

impl<R: Runtime> ConnectionHandler<R> {
    fn new(id: u32, window: tauri::Window<R>) -> Self {
        Self { id, window }
    }

    async fn send_error_to_subscribers(
        &self,
        error: tokio_tungstenite::tungstenite::Error,
    ) -> Result<(), Error> {
        let connections = self.window.state::<ServerConnectionManager>();
        connections
            .send_error_to_subscribers(self.id, Error::ConnectionClosed(self.id, error.to_string()))
            .await
    }

    async fn send_message_to_subscribers(
        &self,
        message: tokio_tungstenite::tungstenite::Message,
    ) -> Result<(), Error> {
        let connections = self.window.state::<ServerConnectionManager>();
        connections
            .send_message_to_subscribers(self.id, message.into())
            .await
    }

    async fn on_shutdown(&self) -> Result<(), Error> {
        let connections = self.window.state::<ServerConnectionManager>();
        connections.remove_connection(self.id).await
    }

    /// Processes incoming WebSocket messages.
    async fn process_messages(
        &self,
        read: &mut SplitStream<tokio_tungstenite::WebSocketStream<TcpStream>>,
    ) -> Result<(), Error> {
        while let Some(message_result) = read.next().await {
            match message_result {
                Ok(message) => self.send_message_to_subscribers(message).await?,
                Err(e) => {
                    self.send_error_to_subscribers(e).await?;
                    break;
                }
            }
        }

        // Remove the connection from the manager
        let connections = self.window.state::<ServerConnectionManager>();
        connections.remove_connection(self.id).await?;

        Ok(())
    }
}
