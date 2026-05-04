// PARTIE 1 — Store de configuration avec versioning

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
    pub value: String,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub namespace: String,
    pub key: String,
    pub value: String,
    pub version: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConfigStore {
    data: HashMap<String, HashMap<String, ConfigValue>>,
    current_version: u64,
}

impl ConfigStore {
    pub fn new() -> Self {
        Self { data: HashMap::new(), current_version: 0 }
    }

    pub fn get(&self, namespace: &str, key: &str) -> Option<&ConfigValue> {
        self.data.get(namespace).and_then(|ns| ns.get(key))
    }

    pub fn set(&mut self, namespace: &str, key: &str, value: String) -> u64 {
        self.current_version += 1;
        let version = self.current_version;
        self.data
            .entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), ConfigValue { value, version });
        version
    }

    pub fn delete(&mut self, namespace: &str, key: &str) -> Option<ConfigValue> {
        if let Some(ns) = self.data.get_mut(namespace) {
            if let Some(val) = ns.remove(key) {
                self.current_version += 1;
                if ns.is_empty() { self.data.remove(namespace); }
                return Some(val);
            }
        }
        None
    }

    pub fn get_namespace(&self, namespace: &str) -> HashMap<String, String> {
        self.data.get(namespace)
            .map(|ns| ns.iter().map(|(k, v)| (k.clone(), v.value.clone())).collect())
            .unwrap_or_default()
    }

    pub fn changes_since(&self, version: u64) -> Vec<ChangeEvent> {
        let mut changes = Vec::new();
        for (ns, keys) in &self.data {
            for (k, v) in keys {
                if v.version > version {
                    changes.push(ChangeEvent {
                        namespace: ns.clone(), key: k.clone(),
                        value: v.value.clone(), version: v.version,
                    });
                }
            }
        }
        changes.sort_by_key(|e| e.version);
        changes
    }

    pub fn current_version(&self) -> u64 { self.current_version }
    pub fn namespaces(&self) -> Vec<String> { self.data.keys().cloned().collect() }
    pub fn namespace_exists(&self, namespace: &str) -> bool { self.data.contains_key(namespace) }
}

pub type SharedConfigStore = Arc<RwLock<ConfigStore>>;

pub fn new_shared_store(capacity: usize) -> (SharedConfigStore, broadcast::Sender<ChangeEvent>) {
    let store = Arc::new(RwLock::new(ConfigStore::new()));
    let (tx, _) = broadcast::channel(capacity);
    (store, tx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut store = ConfigStore::new();
        store.set("db", "host", "localhost".to_string());
        let val = store.get("db", "host").unwrap();
        assert_eq!(val.value, "localhost");
        assert_eq!(val.version, 1);
    }

    #[test]
    fn test_version_increments() {
        let mut store = ConfigStore::new();
        let v1 = store.set("db", "host", "localhost".to_string());
        let v2 = store.set("db", "port", "5432".to_string());
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    #[test]
    fn test_delete() {
        let mut store = ConfigStore::new();
        store.set("db", "host", "localhost".to_string());
        let deleted = store.delete("db", "host");
        assert!(deleted.is_some());
        assert!(store.get("db", "host").is_none());
        assert_eq!(store.current_version(), 2);
    }

    #[test]
    fn test_changes_since() {
        let mut store = ConfigStore::new();
        store.set("db", "host", "localhost".to_string());
        store.set("db", "port", "5432".to_string());
        store.set("app", "url", "http://localhost".to_string());
        let changes = store.changes_since(1);
        assert_eq!(changes.len(), 2);
    }
}