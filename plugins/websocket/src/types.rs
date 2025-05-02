use std::collections::HashMap;

use futures_util::stream::SplitSink;
use serde::{ser::Serializer, Deserialize, Serialize};
use tokio::{net::TcpStream, sync::Mutex};
#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    Connector, MaybeTlsStream, WebSocketStream,
};

pub type Id = u32;
pub type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WebSocketWriter = SplitSink<WebSocket, Message>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("connection not found for the given id: {0}")]
    ConnectionNotFound(Id),
    #[error(transparent)]
    InvalidHeaderValue(#[from] tokio_tungstenite::tungstenite::http::header::InvalidHeaderValue),
    #[error(transparent)]
    InvalidHeaderName(#[from] tokio_tungstenite::tungstenite::http::header::InvalidHeaderName),
    #[error("failed to stop server {0}")]
    FailedToStopServer(Id),
    #[error("server not found for the given id: {0}")]
    ServerNotFound(Id),
    #[error("connection channel closed for the given id: {0}")]
    ConnectionClosed(Id, tauri::Error),
    #[error("invalid message type")]
    InvalidMessageType,
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[cfg(any(feature = "rustls-tls", feature = "native-tls"))]
pub struct TlsConnector(pub Mutex<Option<Connector>>);

#[derive(Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum Max {
    None,
    Number(usize),
}

#[derive(Default)]
pub struct ConnectionManager(pub Mutex<HashMap<Id, WebSocketWriter>>);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionConfig {
    pub read_buffer_size: Option<usize>,
    pub write_buffer_size: Option<usize>,
    pub max_write_buffer_size: Option<usize>,
    pub max_message_size: Option<Max>,
    pub max_frame_size: Option<Max>,
    #[serde(default)]
    pub accept_unmasked_frames: bool,
    pub headers: Option<Vec<(String, String)>>,
}

impl From<ConnectionConfig> for WebSocketConfig {
    fn from(config: ConnectionConfig) -> Self {
        let mut builder =
            WebSocketConfig::default().accept_unmasked_frames(config.accept_unmasked_frames);

        if let Some(read_buffer_size) = config.read_buffer_size {
            builder = builder.read_buffer_size(read_buffer_size)
        }

        if let Some(write_buffer_size) = config.write_buffer_size {
            builder = builder.write_buffer_size(write_buffer_size)
        }

        if let Some(max_write_buffer_size) = config.max_write_buffer_size {
            builder = builder.max_write_buffer_size(max_write_buffer_size)
        }

        if let Some(max_message_size) = config.max_message_size {
            let max_size = match max_message_size {
                Max::None => Option::None,
                Max::Number(n) => Some(n),
            };
            builder = builder.max_message_size(max_size);
        }

        if let Some(max_frame_size) = config.max_frame_size {
            let max_size = match max_frame_size {
                Max::None => Option::None,
                Max::Number(n) => Some(n),
            };
            builder = builder.max_frame_size(max_size);
        }

        builder
    }
}
