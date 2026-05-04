use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use confighub::{
    new_shared_store,
    persistence::ConfigLoader,
    subscription::{SubscriptionServer, SUB_SERVER_PORT},
    http_api::{AppState, build_router, HTTP_SERVER_PORT},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("confighub=info")))
        .init();

    info!("=== ConfigHub démarrage ===");

    let (store, change_tx) = new_shared_store(1024);

    let loader = Arc::new(ConfigLoader::new(
        store.clone(),
        PathBuf::from("config_data"),
        change_tx.clone(),
    ));
    loader.load_initial("CONFIGHUB").await?;

    {
        let s = store.read().await;
        info!("Version du store : {}", s.current_version());
        let ns = s.namespaces();
        if ns.is_empty() { info!("Store vide"); }
        else { info!("Namespaces : {}", ns.join(", ")); }
    }

    let sub_server = SubscriptionServer::new(store.clone(), change_tx.clone(), SUB_SERVER_PORT);

    let http_state = AppState { loader };
    let http_router = build_router(http_state);
    let http_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", HTTP_SERVER_PORT)).await?;

    info!("API HTTP    → http://localhost:{}", HTTP_SERVER_PORT);
    info!("Souscriptions TCP → localhost:{}", SUB_SERVER_PORT);

    tokio::select! {
        r = sub_server.run() => { if let Err(e) = r { eprintln!("Serveur TCP : {}", e); } }
        r = axum::serve(http_listener, http_router) => { if let Err(e) = r { eprintln!("Serveur HTTP : {}", e); } }
    }

    Ok(())
}