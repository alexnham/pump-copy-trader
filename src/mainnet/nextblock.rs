//! NextBlock HTTP submission: direct JSON body and a sensitive Authorization header.
use crate::error::{CopyTraderError, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Client, header::HeaderValue};
use serde::Deserialize;
use serde_json::json;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::time::Duration;
use url::Url;

pub(crate) const TIP_ACCOUNTS: &[&str] = &[
    "NextbLoCkVtMGcV47JzewQdvBpLqT9TxQFozQkN98pE",
    "NexTbLoCkWykbLuB1NkjXgFWkX9oAtcoagQegygXXA2",
    "NeXTBLoCKs9F1y5PJS9CKrFNNLU1keHW71rfh7KgA1X",
    "NexTBLockJYZ7QD7p2byrUa6df8ndV2WSd8GkbWqfbb",
    "neXtBLock1LeC67jYd1QdAa32kbVeubsfPNTJC1V5At",
    "nEXTBLockYgngeRmRrjDV31mGSekVPqZoMGhQEZtPVG",
    "NEXTbLoCkB51HpLBLojQfpyVAMorm3zzKg7w9NFdqid",
    "nextBLoCkPMgmG8ZgJtABeScP35qLa2AMCNKntAP7Xc",
];

pub(crate) fn api_key() -> Result<HeaderValue> {
    let key = std::env::var("NEXTBLOCK_API_KEY").map_err(|_| {
        CopyTraderError::Configuration("NEXTBLOCK_API_KEY is required for NextBlock fanout".into())
    })?;
    if key.trim().is_empty() {
        return Err(CopyTraderError::Configuration(
            "NEXTBLOCK_API_KEY is empty".into(),
        ));
    }
    let mut header = HeaderValue::from_str(key.trim()).map_err(|_| {
        CopyTraderError::Configuration("NEXTBLOCK_API_KEY is not a valid HTTP header".into())
    })?;
    header.set_sensitive(true);
    Ok(header)
}

fn required_key(key: Option<&HeaderValue>) -> Result<&HeaderValue> {
    key.ok_or_else(|| {
        CopyTraderError::Configuration("NEXTBLOCK_API_KEY is missing or invalid".into())
    })
}

#[derive(Deserialize)]
struct SendResponse {
    signature: String,
}

pub(crate) async fn send(
    client: &Client,
    url: &Url,
    key: Option<&HeaderValue>,
    transaction: &Transaction,
    timeout: Duration,
) -> Result<Signature> {
    let bytes = bincode::serialize(transaction)
        .map_err(|_| CopyTraderError::Execution("cannot serialize NextBlock transaction".into()))?;
    let response = client.post(url.clone()).header("Authorization", required_key(key)?.clone()).timeout(timeout)
        .json(&json!({"transaction":{"content":STANDARD.encode(bytes)},"skipPreFlight":true,"frontRunningProtection":false,"disableRetries":true,"revertOnFail":false,"snipeTransaction":false}))
        .send().await.map_err(|_| CopyTraderError::Execution("NextBlock submission transport failure".into()))?;
    if !response.status().is_success() {
        return Err(CopyTraderError::Execution(format!(
            "NextBlock submission HTTP {}",
            response.status().as_u16()
        )));
    }
    let response: SendResponse = response
        .json()
        .await
        .map_err(|_| CopyTraderError::Execution("invalid NextBlock submission response".into()))?;
    response
        .signature
        .parse()
        .map_err(|_| CopyTraderError::Execution("invalid NextBlock response signature".into()))
}

pub(crate) async fn warm(client: &Client, url: &Url, key: Option<&HeaderValue>) -> Result<()> {
    let mut health = url.clone();
    health.set_path("/api/v2/tipfloor");
    health.set_query(None);
    let response = client
        .get(health)
        .header("Authorization", required_key(key)?.clone())
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .map_err(|_| CopyTraderError::Execution("NextBlock health transport failure".into()))?;
    if !response.status().is_success() {
        return Err(CopyTraderError::Execution(
            "NextBlock health HTTP failure".into(),
        ));
    }
    response
        .bytes()
        .await
        .map_err(|_| CopyTraderError::Execution("NextBlock health response failure".into()))?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    pub(crate) async fn server(
        response: serde_json::Value,
        count: usize,
        delay: Duration,
    ) -> (
        Url,
        Arc<Mutex<Vec<(String, serde_json::Value)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/api/v2/submit",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let requests = captured.clone();
        let task = tokio::spawn(async move {
            for _ in 0..count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (offset, length) = loop {
                    let mut buffer = [0; 4096];
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (end + 4, length);
                    }
                };
                while bytes.len() < offset + length {
                    let mut buffer = [0; 4096];
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&buffer[..n]);
                }
                requests.lock().unwrap().push((
                    String::from_utf8_lossy(&bytes[..offset]).into_owned(),
                    if length == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(&bytes[offset..offset + length]).unwrap()
                    },
                ));
                tokio::time::sleep(delay).await;
                let body = response.to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        (url, captured, task)
    }

    #[tokio::test]
    async fn authenticated_health_and_send_preserve_exact_signed_bytes() {
        use solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
        };
        let signer = Keypair::new();
        let tx = Transaction::new_signed_with_payer(
            &[solana_system_interface::instruction::advance_nonce_account(
                &solana_sdk::pubkey::Pubkey::new_unique(),
                &signer.pubkey(),
            )],
            Some(&signer.pubkey()),
            &[&signer],
            Hash::new_unique(),
        );
        let (url, captured, task) = server(
            json!({"signature":tx.signatures[0].to_string(),"result":"ok"}),
            2,
            Duration::ZERO,
        )
        .await;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut key = HeaderValue::from_static("fixture-key");
        key.set_sensitive(true);
        warm(&client, &url, Some(&key)).await.unwrap();
        assert_eq!(
            send(&client, &url, Some(&key), &tx, Duration::from_secs(1))
                .await
                .unwrap(),
            tx.signatures[0]
        );
        task.await.unwrap();
        let requests = captured.lock().unwrap();
        assert!(requests[0].0.starts_with("GET /api/v2/tipfloor "));
        assert!(requests[1].0.starts_with("POST /api/v2/submit "));
        for (header, _) in requests.iter() {
            assert!(header.to_lowercase().contains("authorization: fixture-key"));
        }
        let body = &requests[1].1;
        assert_eq!(body["skipPreFlight"], true);
        assert_eq!(body["disableRetries"], true);
        assert_eq!(body["frontRunningProtection"], false);
        assert_eq!(body["revertOnFail"], false);
        assert_eq!(body["snipeTransaction"], false);
        assert!(body.get("jsonrpc").is_none());
        assert_eq!(
            STANDARD
                .decode(body["transaction"]["content"].as_str().unwrap())
                .unwrap(),
            bincode::serialize(&tx).unwrap()
        );
    }

    #[tokio::test]
    async fn malformed_responses_and_timeouts_do_not_expose_credentials() {
        let client = Client::new();
        let key = HeaderValue::from_static("fixture-secret");
        for (body, delay) in [
            (json!({"signature":"fixture-secret"}), Duration::ZERO),
            (
                json!({"signature":Signature::default().to_string()}),
                Duration::from_millis(200),
            ),
        ] {
            let (url, _, task) = server(body, 1, delay).await;
            let error = send(
                &client,
                &url,
                Some(&key),
                &Transaction::default(),
                Duration::from_millis(30),
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(!error.contains("fixture-secret"));
            task.abort();
        }
    }
}
