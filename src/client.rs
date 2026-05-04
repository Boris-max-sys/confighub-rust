// PARTIE 5 — Client Rust avec cache local et reconnexion automatique

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc, Notify};
use tracing::{info, warn, error};

#[derive(Default)]
struct LocalCache {
    values: HashMap<String, String>,
}
impl LocalCache {
    fn get(&self, ns: &str, key: &str) -> Option<&str> {
        self.values.get(&format!("{}.{}", ns, key)).map(|s| s.as_str())
    }
    fn set(&mut self, ns: &str, key: &str, value: String) {
        self.values.insert(format!("{}.{}", ns, key), value);
    }
    fn remove(&mut self, ns: &str, key: &str) {
        self.values.remove(&format!("{}.{}", ns, key));
    }
}

pub struct ConfigClient {
    http_base: String,
    cache: Arc<RwLock<LocalCache>>,
    update_notify: Arc<Notify>,
    cmd_tx: mpsc::Sender<String>,
    http_client: reqwest::Client,
}

impl ConfigClient {
    pub async fn connect(sub_addr: &str, http_base: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cache = Arc::new(RwLock::new(LocalCache::default()));
        let update_notify = Arc::new(Notify::new());
        let (cmd_tx, cmd_rx) = mpsc::channel::<String>(64);

        let cache_bg = cache.clone();
        let notify_bg = update_notify.clone();
        let addr = sub_addr.to_string();
        tokio::spawn(async move { connection_loop(addr, cache_bg, notify_bg, cmd_rx).await; });

        Ok(Self {
            http_base: http_base.to_string(),
            cache, update_notify, cmd_tx,
            http_client: reqwest::Client::new(),
        })
    }

    pub async fn get(&self, namespace: &str, key: &str) -> Option<String> {
        { let c = self.cache.read().await; if let Some(v) = c.get(namespace, key) { return Some(v.to_string()); } }
        let url = format!("{}/config/{}/{}", self.http_base, namespace, key);
        let resp = self.http_client.get(&url).send().await.ok()?;
        if !resp.status().is_success() { return None; }
        let body: serde_json::Value = resp.json().await.ok()?;
        let value = body["value"].as_str()?.to_string();
        self.cache.write().await.set(namespace, key, value.clone());
        Some(value)
    }

    pub async fn set(&self, namespace: &str, key: &str, value: &str) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/config/{}/{}", self.http_base, namespace, key);
        let resp = self.http_client.put(&url)
            .json(&serde_json::json!({ "value": value }))
            .send().await?;
        if !resp.status().is_success() { return Err(format!("HTTP {}", resp.status()).into()); }
        let body: serde_json::Value = resp.json().await?;
        self.cache.write().await.set(namespace, key, value.to_string());
        Ok(body["version"].as_u64().unwrap_or(0))
    }

    pub async fn delete(&self, namespace: &str, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/config/{}/{}", self.http_base, namespace, key);
        let resp = self.http_client.delete(&url).send().await?;
        let body: serde_json::Value = resp.json().await?;
        self.cache.write().await.remove(namespace, key);
        Ok(body["deleted"].as_bool().unwrap_or(false))
    }

    pub async fn subscribe(&self, pattern: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cmd_tx.send(format!("SUBSCRIBE {}\n", pattern)).await
            .map_err(|e| format!("{}", e).into())
    }

    pub async fn watch<F>(&self, namespace: &str, key: &str, mut callback: F)
    where F: FnMut(String) + Send + 'static {
        let _ = self.subscribe(&format!("{}.{}", namespace, key)).await;
        let cache = self.cache.clone();
        let notify = self.update_notify.clone();
        let ns = namespace.to_string();
        let k = key.to_string();
        let mut last: Option<String> = None;
        loop {
            notify.notified().await;
            let current = { cache.read().await.get(&ns, &k).map(|s| s.to_string()) };
            if let Some(val) = current {
                if last.as_deref() != Some(&val) { last = Some(val.clone()); callback(val); }
            }
        }
    }
}

async fn connection_loop(addr: String, cache: Arc<RwLock<LocalCache>>, notify: Arc<Notify>, mut cmd_rx: mpsc::Receiver<String>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("Connexion au serveur {}", addr);
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = BufReader::new(read_half);
                let mut line = String::new();
                let (inner_tx, mut inner_rx) = mpsc::channel::<String>(32);
                tokio::spawn(async move {
                    while let Some(cmd) = inner_rx.recv().await {
                        if write_half.write_all(cmd.as_bytes()).await.is_err() { break; }
                    }
                });
                loop {
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        if inner_tx.send(cmd).await.is_err() { break; }
                    }
                    line.clear();
                    match tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut line)).await {
                        Ok(Ok(0)) => { warn!("Serveur fermé la connexion"); break; }
                        Ok(Ok(_)) => {
                            let msg = line.trim();
                            if let Some(rest) = msg.strip_prefix("UPDATE ") {
                                let p: Vec<&str> = rest.splitn(4, ' ').collect();
                                if p.len() >= 3 {
                                    let mut c = cache.write().await;
                                    if p[2].is_empty() { c.remove(p[0], p[1]); }
                                    else { c.set(p[0], p[1], p[2].to_string()); }
                                    notify.notify_waiters();
                                }
                            }
                        }
                        Ok(Err(e)) => { error!("Erreur réseau : {}", e); break; }
                        Err(_) => {} // timeout normal
                    }
                }
            }
            Err(e) => warn!("Connexion échouée : {}. Retry dans {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}