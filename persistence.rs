use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use async_std::sync::RwLock;
use std::sync::Arc;
use regex::Regex;
use std::env;

use crate::config_store::{ConfigStore, SharedConfigStore};

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

    pub async fn load_from_files(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir)?;
            return Ok(());
        }

        let mut store = self.store.write().await;

        for entry in fs::read_dir(&self.data_dir)? {
            let path = entry?.path();

            if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                let namespace = path.file_stem().unwrap().to_str().unwrap();

                let content = fs::read_to_string(&path)?;
                let config: NamespaceConfig = toml::from_str(&content)?;

                for (k, v) in config.values {
                    store.set(namespace, &k, v);
                }
            }
        }

        Ok(())
    }

    pub async fn load_from_env(&self, prefix: &str) {
        let mut store = self.store.write().await;
        let re = Regex::new(&format!(r"^{}_([^_]+)_(.+)$", prefix)).unwrap();

        for (key, value) in env::vars() {
            if let Some(cap) = re.captures(&key) {
                let namespace = &cap[1];
                let config_key = &cap[2];
                store.set(namespace, config_key, value);
            }
        }
    }

    pub async fn save_namespace(&self, namespace: &str) -> Result<(), Box<dyn std::error::Error>> {
        let store = self.store.read().await;
        let data = store.get_namespace(namespace);

        if !data.is_empty() {
            let config = NamespaceConfig { values: data };
            let content = toml::to_string_pretty(&config)?;
            let file_path = self.data_dir.join(format!("{}.toml", namespace));
            fs::write(file_path, content)?;
        }

        Ok(())
    }

    pub async fn save_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        let store = self.store.read().await;
        let namespaces = store.namespaces();
        drop(store);

        for ns in namespaces {
            self.save_namespace(&ns).await?;
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

            if !changed {
                break;
            }
        }

        result
    }
}

pub struct ConfigLoader {
    store: SharedConfigStore,
    persistence: ConfigPersistence,
}

impl ConfigLoader {
    pub fn new(store: SharedConfigStore, data_dir: PathBuf) -> Self {
        let persistence = ConfigPersistence::new(store.clone(), data_dir);
        Self { store, persistence }
    }

    pub async fn load_initial(&self, prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.persistence.load_from_files().await?;
        self.persistence.load_from_env(prefix).await;
        Ok(())
    }
    pub async fn set(&self, namespace: &str, key: &str, value: String) -> u64 {
        let snapshot = {
            let store = self.store.read().await;
            store.clone()
        };

        let resolved = ConfigResolver::resolve(&value, &snapshot);

        let mut store = self.store.write().await;
        let version = store.set(namespace, key, resolved);

        drop(store);

        let _ = self.persistence.save_namespace(namespace).await;

        version
    }
}