use futures_util::StreamExt;
use http::header::{HeaderName, HeaderValue};
use tauri::{Manager, Runtime, Window};
#[cfg(any(feature = "rustls-tls", feature = "native-tls"))]
use tokio_tungstenite::connect_async_tls_with_config;
#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use std::str::FromStr;

use crate::{
    server::handle_connection,
    types::{ConnectionConfig, Id, Result, TlsConnector},
};

#[tauri::command]
pub async fn connect<R: Runtime>(
    window: Window<R>,
    url: String,
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

    handle_connection(ws_stream, window, None).await?;

    Ok(id)
}
