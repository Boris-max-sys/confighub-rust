// PARTIE 3 — Serveur de souscription TCP async (port 7878)
// Protocole : SUBSCRIBE ns.key | UNSUBSCRIBE ns.key | PING
// Serveur push : UPDATE namespace key value version

use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn, error, debug};

use crate::config_store::{SharedConfigStore, ChangeEvent};

pub const SUB_SERVER_PORT: u16 = 7879;

struct ClientSession {
    id: u64,
    subscriptions: Arc<RwLock<HashSet<String>>>,
}

impl ClientSession {
    fn new(id: u64) -> Self {
        Self { id, subscriptions: Arc::new(RwLock::new(HashSet::new())) }
    }

    async fn matches(&self, event: &ChangeEvent) -> bool {
        let subs = self.subscriptions.read().await;
        subs.contains(&format!("{}.{}", event.namespace, event.key))
            || subs.contains(&format!("{}.*", event.namespace))
            || subs.contains("*")
    }
}

pub struct SubscriptionServer {
    store: SharedConfigStore,
    change_tx: broadcast::Sender<ChangeEvent>,
    port: u16,
}

impl SubscriptionServer {
    pub fn new(store: SharedConfigStore, change_tx: broadcast::Sender<ChangeEvent>, port: u16) -> Self {
        Self { store, change_tx, port }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        info!("Serveur souscription démarré sur port {}", self.port);
        let mut counter = 0u64;
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    counter += 1;
                    let id = counter;
                    info!("Client #{} connecté depuis {}", id, peer);
                    let rx = self.change_tx.subscribe();
                    let store = self.store.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, id, store, rx).await {
                            debug!("Client #{} déconnecté : {}", id, e);
                        }
                    });
                }
                Err(e) => error!("Erreur accept : {}", e),
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    client_id: u64,
    store: SharedConfigStore,
    mut change_rx: broadcast::Receiver<ChangeEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let session = Arc::new(ClientSession::new(client_id));
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<String>(64);

    // Tâche broadcast → client
    let session_bc = session.clone();
    let tx_bc = msg_tx.clone();
    tokio::spawn(async move {
        loop {
            match change_rx.recv().await {
                Ok(event) => {
                    if session_bc.matches(&event).await {
                        let msg = format!("UPDATE {} {} {} {}\n",
                            event.namespace, event.key, event.value, event.version);
                        if tx_bc.send(msg).await.is_err() { break; }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let _ = tx_bc.send(format!("ERROR Lagged {} events\n", n)).await;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Tâche lecture commandes client
    let session_r = session.clone();
    let tx_r = msg_tx.clone();
    tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let resp = process_command(line.trim(), &session_r, &store).await;
                    if tx_r.send(resp).await.is_err() { break; }
                }
                Err(_) => break,
            }
        }
    });

    // Écriture des messages dans le socket
    while let Some(msg) = msg_rx.recv().await {
        write_half.write_all(msg.as_bytes()).await?;
    }
    Ok(())
}

async fn process_command(cmd: &str, session: &ClientSession, store: &SharedConfigStore) -> String {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    match parts[0].to_uppercase().as_str() {
        "SUBSCRIBE" => {
            if parts.len() < 2 || parts[1].trim().is_empty() {
                return "ERROR Usage: SUBSCRIBE namespace.key\n".to_string();
            }
            let sub = parts[1].trim().to_string();
            let snapshot = { store.read().await.changes_since(0) };
            let mut response = String::new();
            for event in snapshot {
                let matches = sub == format!("{}.{}", event.namespace, event.key)
                    || sub == format!("{}.*", event.namespace)
                    || sub == "*";
                if matches {
                    response.push_str(&format!("UPDATE {} {} {} {}\n",
                        event.namespace, event.key, event.value, event.version));
                }
            }
            session.subscriptions.write().await.insert(sub.clone());
            response.push_str(&format!("OK SUBSCRIBED {}\n", sub));
            response
        }
        "UNSUBSCRIBE" => {
            if parts.len() < 2 { return "ERROR Usage: UNSUBSCRIBE namespace.key\n".to_string(); }
            session.subscriptions.write().await.remove(parts[1].trim());
            format!("OK UNSUBSCRIBED {}\n", parts[1].trim())
        }
        "PING" => "PONG\n".to_string(),
        "" => String::new(),
        other => format!("ERROR Commande inconnue '{}'\n", other),
    }
}