use futures_util::StreamExt;
use log::error;
use tauri::{ipc::Channel, Manager, Runtime};
use tokio::{net::TcpListener, sync::oneshot::Receiver};
use tokio_tungstenite::{accept_async, MaybeTlsStream};

use crate::{
    manager::{ConnectionManager, ServerManager},
    types::{Error, WebSocket, WebSocketReader},
};

/// Starts handling the WebSocket connection.
pub async fn handle_connection<R: Runtime>(
    ws_stream: WebSocket,
    window: tauri::Window<R>,
    on_connection: Option<Channel<u32>>,
) -> Result<(), Error> {
    let (write, mut read) = ws_stream.split();

    // Add the connection to the manager
    let manager = window.state::<ConnectionManager>();
    let id = manager.add_connection(write).await;

    // Inform Tauri about the new connection
    if let Some(channel) = on_connection {
        channel.send(id).map_err(|e| {
            error!("Failed to send connection ID {}: {}", id, e);
            Error::ConnectionClosed(id, e.to_string())
        })?;
    }

    // Handle incoming messages
    let handler = ConnectionHandler::new(id, window.clone());

    tauri::async_runtime::spawn(async move {
        if let Err(e) = handler.process_messages(&mut read).await {
            error!("Error handling connection {}: {}", id, e);
        }

        if let Err(e) = handler.shutdown().await {
            error!("Error shutting down connection {}: {}", id, e);
        }
    });

    Ok(())
}

/// Start handling incoming connections to the server
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
                let stream = match accept_async(MaybeTlsStream::Plain(stream)).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Error accepting connection from {}: {}", addr, e);
                        continue;
                    }
                };

                if let Err(e) = handle_connection(stream,  window.clone(), Some(on_connection.clone())).await {
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
        let connections = self.window.state::<ConnectionManager>();
        connections
            .send_error_to_subscribers(self.id, Error::ConnectionClosed(self.id, error.to_string()))
            .await
    }

    async fn send_message_to_subscribers(
        &self,
        message: tokio_tungstenite::tungstenite::Message,
    ) -> Result<(), Error> {
        let connections = self.window.state::<ConnectionManager>();
        connections
            .send_message_to_subscribers(self.id, message.into())
            .await
    }

    async fn shutdown(&self) -> Result<(), Error> {
        let connections = self.window.state::<ConnectionManager>();
        connections.remove_connection(self.id).await
    }

    /// Processes incoming WebSocket messages.
    async fn process_messages(&self, read: &mut WebSocketReader) -> Result<(), Error> {
        while let Some(message_result) = read.next().await {
            match message_result {
                Ok(message) => self.send_message_to_subscribers(message).await?,
                Err(e) => {
                    self.send_error_to_subscribers(e).await?;
                    break;
                }
            }
        }

        Ok(())
    }
}
