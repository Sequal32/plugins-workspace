use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::{protocol::CloseFrame as ProtocolCloseFrame, Message};

#[derive(Deserialize, Serialize, Clone)]
/// A representation of a Websocket [ProtocolCloseFrame] that can be sent over Tauri
pub struct CloseFrame {
    pub code: u16,
    pub reason: String,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
/// A representation of a Websocket [Message] that can be sent over Tauri
pub enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<CloseFrame>),
    Error(String),
}

impl From<Message> for WebSocketMessage {
    fn from(message: Message) -> Self {
        match message {
            Message::Text(t) => WebSocketMessage::Text(t.to_string()),
            Message::Binary(t) => WebSocketMessage::Binary(t.to_vec()),
            Message::Ping(t) => WebSocketMessage::Ping(t.to_vec()),
            Message::Pong(t) => WebSocketMessage::Pong(t.to_vec()),
            Message::Close(t) => WebSocketMessage::Close(t.map(|v| CloseFrame {
                code: v.code.into(),
                reason: v.reason.to_string(),
            })),
            Message::Frame(_) => panic!("Never should be read"),
        }
    }
}

impl From<WebSocketMessage> for Message {
    fn from(val: WebSocketMessage) -> Self {
        match val {
            WebSocketMessage::Text(t) => Message::Text(t.into()),
            WebSocketMessage::Binary(t) => Message::Binary(t.into()),
            WebSocketMessage::Ping(t) => Message::Ping(t.into()),
            WebSocketMessage::Pong(t) => Message::Pong(t.into()),
            WebSocketMessage::Close(t) => Message::Close(t.map(|v| ProtocolCloseFrame {
                code: v.code.into(),
                reason: v.reason.to_string().into(),
            })),
            WebSocketMessage::Error(_) => panic!("Never should be converted to"),
        }
    }
}
