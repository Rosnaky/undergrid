#[derive(Debug, Clone)]
pub enum SchedulerError {
    Fail(String),
    InsufficientResources(String),
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchedulerError::Fail(msg) => write!(f, "Scheduler failure occurred: {}", msg),
            SchedulerError::InsufficientResources(msg) => write!(f, "Scheduler insufficient resources: {}", msg),
        }
    }
}

impl std::error::Error for SchedulerError {}
