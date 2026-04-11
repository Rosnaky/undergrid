use crate::task::TaskId;

#[derive(Debug, Clone)]
pub enum JobError {
    Invalid(String),
    MissingDependency(TaskId, TaskId),
    CycleDetected,
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::Invalid(msg) => write!(f, "Job is invalid: {}", msg),
            JobError::MissingDependency(task_id, missing_dep) => write!(
                f,
                "The dependency {} is missing for task {}: ",
                task_id, missing_dep
            ),
            JobError::CycleDetected => write!(f, "A cycle was detected"),
        }
    }
}

impl std::error::Error for JobError {}
