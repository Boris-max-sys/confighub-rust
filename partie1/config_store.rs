use std::collections::HashMap;
use std::sync::Arc;
use async_std::sync::RwLock;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
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
        Self {
            data: HashMap::new(),
            current_version: 0,
        }
    }

    pub fn get(&self, namespace: &str, key: &str) -> Option<&ConfigValue> {
        self.data.get(namespace).and_then(|ns| ns.get(key))
    }

    pub fn set(&mut self, namespace: &str, key: &str, value: String) -> u64 {
        self.current_version += 1;
        let version = self.current_version;

        let config_value = ConfigValue { value, version };

        self.data
            .entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), config_value);

        version
    }

    pub fn delete(&mut self, namespace: &str, key: &str) -> Option<ConfigValue> {
        if let Some(ns) = self.data.get_mut(namespace) {
            if let Some(val) = ns.remove(key) {
                self.current_version += 1;
                return Some(val);
            }
        }
        None
    }

    pub fn get_namespace(&self, namespace: &str) -> HashMap<String, String> {
        self.data
            .get(namespace)
            .map(|ns| {
                ns.iter()
                    .map(|(k, v)| (k.clone(), v.value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn changes_since(&self, version: u64) -> Vec<(String, String, String, u64)> {
        let mut changes = Vec::new();

        for (ns, keys) in &self.data {
            for (k, v) in keys {
                if v.version > version {
                    changes.push((ns.clone(), k.clone(), v.value.clone(), v.version));
                }
            }
        }

        changes.sort_by_key(|(_, _, _, v)| *v);
        changes
    }

    pub fn current_version(&self) -> u64 {
        self.current_version
    }

    pub fn namespaces(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }
}

pub type SharedConfigStore = Arc<RwLock<ConfigStore>>;