#[derive(Debug, Clone)]
pub enum SystemError {
    ReadError(String),
    ParseError(String),
}

impl std::fmt::Display for SystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemError::ReadError(msg) => write!(f, "Read error: {}", msg),
            SystemError::ParseError(msg) => write!(f, "Parsing error: {}", msg),
        }
    }
}

impl std::error::Error for SystemError {}
