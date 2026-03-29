use serde::{Serialize, Deserialize};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum ConfigError {
    NoConfig(String),
    ConfigAlreadyExists(String),
    SerializationError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::NoConfig(msg) => write!(f, "No configuration: {}", msg),
            ConfigError::ConfigAlreadyExists(msg) => write!(f, "Configuration already exists: {}", msg),
            ConfigError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub bind_address: String,
    pub port: u16,
    pub heartbeat_interval_secs: u64,
}

impl NodeConfig {
    /// Returns ~/.undergrid/config.toml
    fn path() -> Result<PathBuf, ConfigError> {
        let home = dirs::home_dir
            .map_err(|e| ConfigError::NoConfig(format!("HOME not set: {}", e)))?;
        Ok(PathBuf::from(home).join(".undergrid").join("config.toml"))
    }

    /// Read config file if it exists on disk
    pub fn load() -> Result<Self, ConfigError> {
        let path = Self::path()?;

        let config_toml = fs::read_to_string(&path)
            .map_err(|e| ConfigError::NoConfig(e.to_string()))?;

        let config: NodeConfig = toml::from_str(&config_toml)
            .map_err(|e| ConfigError::SerializationError(e.to_string()))?;

        Ok(config)
    }

    /// Create config file with defaults if it does not exist on disk
    pub fn create() -> Result<Self, ConfigError> {
        let path = Self::path()?;

        if path.exists() {
            return Err(ConfigError::ConfigAlreadyExists(
                path.display().to_string(),
            ));
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
    pub fn load_or_create() -> Result<Self, ConfigError> {
        match Self::load() {
            Ok(config) => Ok(config),
            Err(ConfigError::NoConfig(_)) => Self::create(),
            Err(e) => Err(e),
        }
    }
}
