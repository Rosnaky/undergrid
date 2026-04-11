#[derive(Debug, Clone)]
pub enum ExecutorError {
    ExecutionFailed(String),
    ImagePullFailed(String),
    Timeout,
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::ExecutionFailed(msg) => write!(f, "Execution of task failed: {}", msg),
            ExecutorError::ImagePullFailed(msg) => {
                write!(f, "Container image pulling failed: {}", msg)
            }
            ExecutorError::Timeout => write!(f, "Execution of task timed out"),
        }
    }
}

impl std::error::Error for ExecutorError {}
