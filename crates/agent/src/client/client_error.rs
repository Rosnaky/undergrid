#[derive(Debug, Clone)]
pub enum ClientError {
    ConnectionError(String),
    QueryError(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            ClientError::QueryError(msg) => write!(f, "Connection error: {}", msg),
        }
    }
}

impl std::error::Error for ClientError {}
