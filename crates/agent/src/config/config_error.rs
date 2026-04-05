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
            ConfigError::ConfigAlreadyExists(msg) => {
                write!(f, "Configuration already exists: {}", msg)
            }
            ConfigError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}
