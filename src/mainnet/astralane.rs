//! Astralane Iris JSON-RPC submission. Credentials remain in a sensitive header.
use crate::error::{CopyTraderError, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Client, header::HeaderValue};
use serde_json::json;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::time::Duration;
use url::Url;
pub(crate) const TIP_ACCOUNTS: &[&str] = &[
    "astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF",
    "astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm",
    "astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk",
    "astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL",
    "astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK",
    "astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC",
    "astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk",
    "astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK",
];
pub(crate) fn api_key() -> Result<HeaderValue> {
    let key = std::env::var("ASTRALANE_API_KEY").map_err(|_| {
        CopyTraderError::Configuration("ASTRALANE_API_KEY is required for Astralane fanout".into())
    })?;
    if key.trim().is_empty() {
        return Err(CopyTraderError::Configuration(
            "ASTRALANE_API_KEY is empty".into(),
        ));
    }
    let mut header = HeaderValue::from_str(key.trim()).map_err(|_| {
        CopyTraderError::Configuration("ASTRALANE_API_KEY is not a valid HTTP header".into())
    })?;
    header.set_sensitive(true);
    Ok(header)
}

fn required_key(key: Option<&HeaderValue>) -> Result<&HeaderValue> {
    key.ok_or_else(|| {
        CopyTraderError::Configuration("ASTRALANE_API_KEY is missing or invalid".into())
    })
}

pub(crate) async fn send(
    client: &Client,
    url: &Url,
    key: Option<&HeaderValue>,
    transaction: &Transaction,
    timeout: Duration,
) -> Result<Signature> {
    let bytes = bincode::serialize(transaction)
        .map_err(|_| CopyTraderError::Execution("cannot serialize Astralane transaction".into()))?;
    let body = json!({"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":[STANDARD.encode(bytes),{"encoding":"base64","skipPreflight":true,"maxRetries":0}]});
    let response = client
        .post(url.clone())
        .header("api_key", required_key(key)?.clone())
        .timeout(timeout)
        .json(&body)
        .send()
        .await
        .map_err(|_| CopyTraderError::Execution("Astralane submission transport failure".into()))?;
    if !response.status().is_success() {
        return Err(CopyTraderError::Execution(format!(
            "Astralane submission HTTP {}",
            response.status().as_u16()
        )));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|_| CopyTraderError::Execution("invalid Astralane response".into()))?;
    if body.get("error").is_some() {
        return Err(CopyTraderError::Execution(
            "Astralane submission rejected".into(),
        ));
    }
    body.get("result")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CopyTraderError::Execution("invalid Astralane response signature".into()))
}
pub(crate) async fn warm(client: &Client, url: &Url, key: Option<&HeaderValue>) -> Result<()> {
    let response = client
        .post(url.clone())
        .header("api_key", required_key(key)?.clone())
        .timeout(Duration::from_secs(2))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}))
        .send()
        .await
        .map_err(|_| CopyTraderError::Execution("Astralane health transport failure".into()))?;
    if !response.status().is_success() {
        return Err(CopyTraderError::Execution(
            "Astralane health HTTP failure".into(),
        ));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|_| CopyTraderError::Execution("invalid Astralane health response".into()))?;
    if body.get("error").is_some() {
        return Err(CopyTraderError::Execution(
            "Astralane health rejected".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    #[tokio::test]
    async fn iris_submits_exact_signed_bytes_with_header_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/iris", listener.local_addr().unwrap())).unwrap();
        let signature = Signature::new_unique();
        let mut tx = Transaction::default();
        tx.signatures = vec![signature];
        let expected = STANDARD.encode(bincode::serialize(&tx).unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (offset, length) = loop {
                let mut buffer = [0; 4096];
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                    assert!(headers.starts_with("post /iris "));
                    assert!(headers.contains("api_key: fixture-key"));
                    assert!(!headers.contains("api-key="));
                    let length = headers
                        .lines()
                        .find_map(|l| {
                            l.strip_prefix("content-length:")
                                .map(|n| n.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while bytes.len() < offset + length {
                let mut buffer = [0; 4096];
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let body: serde_json::Value =
                serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
            assert_eq!(body["method"], "sendTransaction");
            assert_eq!(body["params"][0], expected);
            assert_eq!(
                body["params"][1],
                json!({"encoding":"base64","skipPreflight":true,"maxRetries":0})
            );
            let reply = json!({"jsonrpc":"2.0","id":1,"result":signature.to_string()}).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",reply.len(),reply).as_bytes()).await.unwrap();
        });
        let mut key = HeaderValue::from_static("fixture-key");
        key.set_sensitive(true);
        assert_eq!(
            send(
                &Client::new(),
                &url,
                Some(&key),
                &tx,
                Duration::from_secs(2)
            )
            .await
            .unwrap(),
            signature
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn missing_credentials_fail_without_exposing_details() {
        let err = send(
            &Client::new(),
            &Url::parse("https://ny.gateway.astralane.io/iris").unwrap(),
            None,
            &Transaction::default(),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("ASTRALANE_API_KEY"));
    }
}
