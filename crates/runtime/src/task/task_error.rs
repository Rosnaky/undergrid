#[derive(Debug, Clone)]
pub enum TaskError {
    InvalidStateTransition(String),
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::InvalidStateTransition(msg) => {
                write!(f, "Invalid state transition: {}", msg)
            }
        }
    }
}

impl std::error::Error for TaskError {}
