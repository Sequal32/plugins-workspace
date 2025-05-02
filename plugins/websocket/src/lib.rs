// Copyright 2019-2023 Tauri Programme within The Commons Conservancy
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: MIT

//! Open a WebSocket connection using a Rust client in JS.

#![doc(
    html_logo_url = "https://github.com/tauri-apps/tauri/raw/dev/app-icon.png",
    html_favicon_url = "https://github.com/tauri-apps/tauri/raw/dev/app-icon.png"
)]

use server::ServerManager;
use tauri::{
    plugin::{Builder as PluginBuilder, TauriPlugin},
    Manager, Runtime,
};
use tokio::sync::Mutex;
use tokio_tungstenite::Connector;

mod client;
mod handler;
mod message;
mod server;
mod types;

use client::{connect, send};
use server::{new_server, send_server_conn, stop_server, subscribe_server};
use types::{ConnectionManager, TlsConnector};

pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::default().build()
}

#[derive(Default)]
pub struct Builder {
    tls_connector: Option<Connector>,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            tls_connector: None,
        }
    }

    pub fn tls_connector(mut self, connector: Connector) -> Self {
        self.tls_connector.replace(connector);
        self
    }

    pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
        PluginBuilder::new("websocket")
            .invoke_handler(tauri::generate_handler![
                connect,
                send,
                new_server,
                stop_server,
                send_server_conn,
                subscribe_server
            ])
            .setup(|app, _api| {
                app.manage(ConnectionManager::default());
                app.manage(ServerManager::default());
                #[cfg(any(feature = "rustls-tls", feature = "native-tls"))]
                app.manage(TlsConnector(Mutex::new(self.tls_connector)));
                Ok(())
            })
            .build()
    }
}
