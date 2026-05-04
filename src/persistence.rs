// PARTIE 2 — Persistance TOML, variables d'environnement, résolution ${ref}

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use regex::Regex;
use std::env;
use tokio::sync::broadcast;
use tracing::{info, warn, error};

use crate::config_store::{ConfigStore, SharedConfigStore, ChangeEvent};

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceConfig {
    values: HashMap<String, String>,
}

pub struct ConfigPersistence {
    store: SharedConfigStore,
    data_dir: PathBuf,
}

impl ConfigPersistence {
    pub fn new(store: SharedConfigStore, data_dir: PathBuf) -> Self {
        Self { store, data_dir }
    }

    pub async fn load_from_files(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir)?;
            return Ok(());
        }
        let mut store = self.store.write().await;
        for entry in fs::read_dir(&self.data_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                let namespace = path.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown").to_string();
                match fs::read_to_string(&path) {
                    Ok(content) => match toml::from_str::<NamespaceConfig>(&content) {
                        Ok(config) => {
                            for (k, v) in config.values { store.set(&namespace, &k, v); }
                            info!("Namespace '{}' chargé", namespace);
                        }
                        Err(e) => warn!("Erreur parsing TOML {:?} : {}", path, e),
                    },
                    Err(e) => error!("Impossible de lire {:?} : {}", path, e),
                }
            }
        }
        Ok(())
    }

    pub async fn load_from_env(&self, prefix: &str) {
        let mut store = self.store.write().await;
        let re = Regex::new(&format!(r"^{}_([^_]+)_(.+)$", regex::escape(prefix))).unwrap();
        for (key, value) in env::vars() {
            if let Some(caps) = re.captures(&key) {
                store.set(&caps[1].to_lowercase(), &caps[2].to_lowercase(), value);
            }
        }
    }

    pub async fn save_namespace(&self, namespace: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let store = self.store.read().await;
        let data = store.get_namespace(namespace);
        if data.is_empty() { return Ok(()); }
        fs::create_dir_all(&self.data_dir)?;
        let content = toml::to_string_pretty(&NamespaceConfig { values: data })?;
        fs::write(self.data_dir.join(format!("{}.toml", namespace)), content)?;
        Ok(())
    }

    pub async fn save_all(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let namespaces = { self.store.read().await.namespaces() };
        for ns in namespaces {
            let _ = self.save_namespace(&ns).await;
        }
        Ok(())
    }
}

pub struct ConfigResolver;

impl ConfigResolver {
    pub fn resolve(value: &str, snapshot: &ConfigStore) -> String {
        let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
        let mut result = value.to_string();
        for _ in 0..10 {
            let mut changed = false;
            let new_value = re.replace_all(&result, |caps: &regex::Captures| {
                let expr = &caps[1];
                if let Some((ns, key)) = expr.split_once('.') {
                    if let Some(v) = snapshot.get(ns, key) {
                        changed = true;
                        return v.value.clone();
                    }
                }
                caps[0].to_string()
            });
            result = new_value.to_string();
            if !changed { break; }
        }
        result
    }
}

pub struct ConfigLoader {
    pub store: SharedConfigStore,
    pub persistence: ConfigPersistence,
    pub change_tx: broadcast::Sender<ChangeEvent>,
}

impl ConfigLoader {
    pub fn new(store: SharedConfigStore, data_dir: impl AsRef<Path>, change_tx: broadcast::Sender<ChangeEvent>) -> Self {
        let persistence = ConfigPersistence::new(store.clone(), data_dir.as_ref().to_path_buf());
        Self { store, persistence, change_tx }
    }

    pub async fn load_initial(&self, env_prefix: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.persistence.load_from_files().await?;
        self.persistence.load_from_env(env_prefix).await;
        Ok(())
    }

    pub async fn set(&self, namespace: &str, key: &str, value: String) -> u64 {
        let resolved = { let s = self.store.read().await; ConfigResolver::resolve(&value, &s) };
        let version = { let mut s = self.store.write().await; s.set(namespace, key, resolved.clone()) };
        let _ = self.change_tx.send(ChangeEvent {
            namespace: namespace.to_string(), key: key.to_string(),
            value: resolved, version,
        });
        let _ = self.persistence.save_namespace(namespace).await;
        version
    }

    pub async fn delete(&self, namespace: &str, key: &str) -> Option<String> {
        let deleted = { let mut s = self.store.write().await; s.delete(namespace, key) };
        if let Some(ref val) = deleted {
            let version = { self.store.read().await.current_version() };
            let _ = self.change_tx.send(ChangeEvent {
                namespace: namespace.to_string(), key: key.to_string(),
                value: String::new(), version,
            });
            let _ = self.persistence.save_namespace(namespace).await;
            Some(val.value.clone())
        } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::ConfigStore;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_resolve_simple() {
        let mut store = ConfigStore::new();
        store.set("db", "host", "localhost".to_string());
        store.set("db", "port", "5432".to_string());
        assert_eq!(ConfigResolver::resolve("${db.host}:${db.port}", &store), "localhost:5432");
    }

    #[test]
    fn test_resolve_unknown() {
        let store = ConfigStore::new();
        assert_eq!(ConfigResolver::resolve("${unknown.key}", &store), "${unknown.key}");
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let tmp = TempDir::new().unwrap();
        let store1 = Arc::new(tokio::sync::RwLock::new(ConfigStore::new()));
        { let mut s = store1.write().await; s.set("db", "host", "localhost".to_string()); }
        let p = ConfigPersistence::new(store1.clone(), tmp.path().to_path_buf());
        p.save_namespace("db").await.unwrap();

        let store2 = Arc::new(tokio::sync::RwLock::new(ConfigStore::new()));
        let p2 = ConfigPersistence::new(store2.clone(), tmp.path().to_path_buf());
        p2.load_from_files().await.unwrap();
        let s2 = store2.read().await;
        assert_eq!(s2.get("db", "host").unwrap().value, "localhost");
    }
}