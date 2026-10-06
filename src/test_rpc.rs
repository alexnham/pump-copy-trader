//! Loopback-only JSON-RPC fixture; never contacts a Solana network.
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

pub struct TestRpc {
    pub url: url::Url,
    pub requests: Arc<Mutex<Vec<Value>>>,
    pub peak_in_flight: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl TestRpc {
    pub async fn start(handler: impl Fn(&Value) -> Value + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test RPC");
        let url = url::Url::parse(&format!(
            "http://{}",
            listener.local_addr().expect("address")
        ))
        .expect("url");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let handler = Arc::new(handler);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak_in_flight = Arc::new(AtomicUsize::new(0));
        let peak = peak_in_flight.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.expect("accept");
                        let recorded = recorded.clone();
                        let handler = handler.clone();
                        let in_flight = in_flight.clone();
                        let peak = peak.clone();
                        connections.spawn(async move {
                            let mut bytes = Vec::new();
                            let (header_end, length) = loop {
                                let mut buffer = [0; 4096];
                                let n = socket.read(&mut buffer).await.expect("read headers");
                                if n == 0 { return; }
                                bytes.extend_from_slice(&buffer[..n]);
                                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                                    let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                                    let length = headers.lines().find_map(|line| line.strip_prefix("content-length:").map(|value| value.trim().parse::<usize>().expect("length"))).unwrap_or(0);
                                    break (end + 4, length);
                                }
                            };
                            while bytes.len() < header_end + length {
                                let mut buffer = [0; 4096];
                                let n = socket.read(&mut buffer).await.expect("read body");
                                if n == 0 { return; }
                                bytes.extend_from_slice(&buffer[..n]);
                            }
                            let request = if length == 0 { json!({"method":"ping"}) } else { serde_json::from_slice(&bytes[header_end..header_end + length]).expect("JSON request") };
                            recorded.lock().expect("requests").push(request.clone());
                            let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(active, Ordering::SeqCst);
                            let result = handler(&request);
                            let delay = result.get("test_delay_ms").and_then(Value::as_u64).unwrap_or(0);
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                            in_flight.fetch_sub(1, Ordering::SeqCst);
                            let body = if result.get("error").is_some() { json!({"jsonrpc":"2.0","id":request["id"],"error":result["error"]}) } else { json!({"jsonrpc":"2.0","id":request["id"],"result":result.get("test_result").unwrap_or(&result)}) }.to_string();
                            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                            let _ = socket.write_all(response.as_bytes()).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url,
            requests,
            peak_in_flight,
            task,
        }
    }
    pub fn count(&self, method: &str) -> usize {
        self.requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|r| r["method"] == method)
            .count()
    }
}
impl Drop for TestRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn mint_account() -> Value {
    use base64::Engine;
    let mut data = vec![0_u8; 82];
    data[44] = 6;
    data[45] = 1;
    json!({"lamports":1,"owner":spl_token::id().to_string(),"data":[base64::engine::general_purpose::STANDARD.encode(data),"base64"],"executable":false,"rentEpoch":0})
}
