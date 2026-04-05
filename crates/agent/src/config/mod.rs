pub mod config_error;

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

use crate::config::config_error::ConfigError;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub bind_address: String,
    pub port: u16,
    pub heartbeat_interval_secs: u64,
}

impl NodeConfig {
    /// Returns ~/.undergrid/config.toml
    fn path(port: u16) -> Result<PathBuf, ConfigError> {
        let home = dirs::home_dir()
            .ok_or_else(|| ConfigError::NoConfig("HOME directory not found".into()))?;
        Ok(home
            .join(".undergrid")
            .join(format!("config-{}.toml", port)))
    }

    /// Read config file if it exists on disk
    pub fn load(port: u16) -> Result<Self, ConfigError> {
        let path = Self::path(port)?;

        let config_toml =
            fs::read_to_string(&path).map_err(|e| ConfigError::NoConfig(e.to_string()))?;

        let config: NodeConfig = toml::from_str(&config_toml)
            .map_err(|e| ConfigError::SerializationError(e.to_string()))?;

        Ok(config)
    }

    /// Create config file with defaults if it does not exist on disk
    pub fn create(port: u16) -> Result<Self, ConfigError> {
        let path = Self::path(port)?;

        if path.exists() {
            return Err(ConfigError::ConfigAlreadyExists(path.display().to_string()));
        }

        let config = NodeConfig {
            node_id: Uuid::new_v4().to_string(),
            bind_address: "0.0.0.0".to_string(),
            port: 7070,
            heartbeat_interval_secs: 5,
        };

        // Ensure ~/.undergrid/ directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| ConfigError::NoConfig(format!("Could not create dir: {}", e)))?;
        }

        let toml_string = toml::to_string_pretty(&config)
            .map_err(|e| ConfigError::SerializationError(e.to_string()))?;

        fs::write(&path, toml_string)
            .map_err(|e| ConfigError::NoConfig(format!("Could not write config: {}", e)))?;

        Ok(config)
    }

    /// Load existing config, or create a new one if none exists
    pub fn load_or_create(port: u16) -> Result<Self, ConfigError> {
        match Self::load(port) {
            Ok(config) => Ok(config),
            Err(ConfigError::NoConfig(_)) => Self::create(port),
            Err(e) => Err(e),
        }
    }
}
