use std::net::SocketAddr;

use tauri::{ipc::Channel, Runtime};
use tokio::net::TcpListener;

use crate::{
    manager::{ServerConnectionManager, ServerManager},
    message::WebSocketMessage,
    server::handle_server,
    types::Id,
};

#[tauri::command]
pub async fn stop_server(
    id: Id,
    server_manager: tauri::State<'_, ServerManager>,
) -> Result<(), String> {
    match server_manager.remove_server(id).await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Failed to stop server {}: {}", id, e)),
    }
}

#[tauri::command]
pub async fn subscribe_server(
    id: Id,
    on_message: Channel<serde_json::Value>,
    server_manager: tauri::State<'_, ServerConnectionManager>,
) -> Result<(), String> {
    match server_manager.subscribe(id, on_message).await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Failed to subscribe to server {}: {}", id, e)),
    }
}

#[tauri::command]
pub async fn send_server_conn(
    id: Id,
    message: WebSocketMessage,
    server_manager: tauri::State<'_, ServerConnectionManager>,
) -> Result<(), String> {
    match server_manager.send_message(id, message.into()).await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Failed to send message to server {}: {}", id, e)),
    }
}

#[tauri::command]
pub async fn new_server<R: Runtime>(
    port: u16,
    window: tauri::Window<R>,
    on_connection: Channel<u32>,
    server_manager: tauri::State<'_, ServerManager>,
) -> Result<u32, String> {
    let listen = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port)))
        .await
        .map_err(|e| format!("Failed to bind to port {}: {}", port, e))?;

    let (id, kill_req) = server_manager.add_server().await;

    tauri::async_runtime::spawn(handle_server(listen, id, kill_req, window, on_connection));

    Ok(id)
}
