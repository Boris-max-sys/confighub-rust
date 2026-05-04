mod config_store;
mod persistence;

use std::sync::Arc;
use async_std::sync::RwLock;
use std::path::PathBuf;

use config_store::ConfigStore;
use persistence::ConfigLoader;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ConfigHub démarrage...");

    let store = Arc::new(RwLock::new(ConfigStore::new()));

    let loader = ConfigLoader::new(store.clone(), PathBuf::from("config"));

    loader.load_initial("APP").await?;

    println!("✅ Config chargée");

    loader.set("db", "host", "localhost".to_string()).await;
    loader.set("db", "port", "5432".to_string()).await;

    loader.set("app", "url", "postgres://${db.host}:${db.port}".to_string()).await;

    {
        let store_read = store.read().await;

        if let Some(val) = store_read.get("app", "url") {
            println!("🌐 URL finale = {}", val.value);
        }

        println!("📊 Version actuelle = {}", store_read.current_version());

        let changes = store_read.changes_since(0);
        println!("🔄 Changements depuis version 0 :");
        for (ns, key, value, version) in changes {
            println!("  [{}] {} = {} (v{})", ns, key, value, version);
        }
    }

    println!("💾 Sauvegarde automatique déjà faite");

    Ok(())
}