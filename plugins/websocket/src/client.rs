use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderName, HeaderValue};
use tauri::{ipc::Channel, Manager, Runtime, State, Window};
#[cfg(any(feature = "rustls-tls", feature = "native-tls"))]
use tokio_tungstenite::connect_async_tls_with_config;
#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest, protocol::CloseFrame as ProtocolCloseFrame, Message,
};

use std::str::FromStr;

use crate::{
    message::{CloseFrame, WebSocketMessage},
    types::{ConnectionConfig, ConnectionManager, Error, Id, Result, TlsConnector},
};

#[tauri::command]
pub async fn connect<R: Runtime>(
    window: Window<R>,
    url: String,
    on_message: Channel<serde_json::Value>,
    config: Option<ConnectionConfig>,
) -> Result<Id> {
    let id = rand::random();
    let mut request = url.into_client_request()?;

    if let Some(headers) = config.as_ref().and_then(|c| c.headers.as_ref()) {
        for (k, v) in headers {
            let header_name = HeaderName::from_str(k.as_str())?;
            let header_value = HeaderValue::from_str(v.as_str())?;
            request.headers_mut().insert(header_name, header_value);
        }
    }

    #[cfg(any(feature = "rustls-tls", feature = "native-tls"))]
    let tls_connector = match window.try_state::<TlsConnector>() {
        Some(tls_connector) => tls_connector.0.lock().await.clone(),
        None => None,
    };

    #[cfg(any(feature = "rustls-tls", feature = "native-tls"))]
    let (ws_stream, _) =
        connect_async_tls_with_config(request, config.map(Into::into), false, tls_connector)
            .await?;
    #[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
    let (ws_stream, _) = connect_async_with_config(request, config.map(Into::into), false).await?;

    tauri::async_runtime::spawn(async move {
        let (write, read) = ws_stream.split();
        let manager = window.state::<ConnectionManager>();
        manager.0.lock().await.insert(id, write);
        read.for_each(move |message| {
            let window_ = window.clone();
            let on_message_ = on_message.clone();
            async move {
                if let Ok(Message::Close(_)) = message {
                    let manager = window_.state::<ConnectionManager>();
                    manager.0.lock().await.remove(&id);
                }

                let response = match message {
                    Ok(Message::Text(t)) => {
                        serde_json::to_value(WebSocketMessage::Text(t.to_string())).unwrap()
                    }
                    Ok(Message::Binary(t)) => {
                        serde_json::to_value(WebSocketMessage::Binary(t.to_vec())).unwrap()
                    }
                    Ok(Message::Ping(t)) => {
                        serde_json::to_value(WebSocketMessage::Ping(t.to_vec())).unwrap()
                    }
                    Ok(Message::Pong(t)) => {
                        serde_json::to_value(WebSocketMessage::Pong(t.to_vec())).unwrap()
                    }
                    Ok(Message::Close(t)) => {
                        serde_json::to_value(WebSocketMessage::Close(t.map(|v| CloseFrame {
                            code: v.code.into(),
                            reason: v.reason.to_string(),
                        })))
                        .unwrap()
                    }
                    Ok(Message::Frame(_)) => serde_json::Value::Null, // This value can't be recieved.
                    Err(e) => serde_json::to_value(Error::from(e)).unwrap(),
                };

                let _ = on_message_.send(response);
            }
        })
        .await;
    });

    Ok(id)
}

#[tauri::command]
pub async fn send(
    manager: State<'_, ConnectionManager>,
    id: Id,
    message: WebSocketMessage,
) -> Result<()> {
    if let Some(write) = manager.0.lock().await.get_mut(&id) {
        write
            .send(match message {
                WebSocketMessage::Text(t) => Message::Text(t.into()),
                WebSocketMessage::Binary(t) => Message::Binary(t.into()),
                WebSocketMessage::Ping(t) => Message::Ping(t.into()),
                WebSocketMessage::Pong(t) => Message::Pong(t.into()),
                WebSocketMessage::Close(t) => Message::Close(t.map(|v| ProtocolCloseFrame {
                    code: v.code.into(),
                    reason: v.reason.into(),
                })),
            })
            .await?;
        Ok(())
    } else {
        Err(Error::ConnectionNotFound(id))
    }
}
