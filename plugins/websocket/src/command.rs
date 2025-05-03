use std::net::SocketAddr;

use log::debug;
use tauri::{ipc::Channel, Runtime, State};
use tokio::net::TcpListener;

use crate::{
    manager::{ConnectionManager, ServerManager},
    message::WebSocketMessage,
    server::handle_server,
    types::{Id, Result},
};

#[tauri::command]
pub async fn stop_server(id: Id, server_manager: State<'_, ServerManager>) -> Result<()> {
    server_manager.remove_server(id).await
}

#[tauri::command]
pub async fn subscribe(
    id: Id,
    on_message: Channel<WebSocketMessage>,
    server_manager: State<'_, ConnectionManager>,
) -> Result<()> {
    server_manager.subscribe(id, on_message).await
}

#[tauri::command]
pub async fn send(
    manager: State<'_, ConnectionManager>,
    id: Id,
    message: WebSocketMessage,
) -> Result<()> {
    manager.send_message(id, message.into()).await
}

#[tauri::command]
pub async fn new_server<R: Runtime>(
    port: u16,
    window: tauri::Window<R>,
    on_connection: Channel<u32>,
    server_manager: State<'_, ServerManager>,
) -> Result<u32> {
    let listen = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;

    debug!("Listening on {}", listen.local_addr()?);

    let (id, kill_req) = server_manager.add_server().await;

    debug!("New WebSocket server created with ID: {}", id);

    tauri::async_runtime::spawn(handle_server(listen, id, kill_req, window, on_connection));

    Ok(id)
}
