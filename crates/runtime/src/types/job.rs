
use crate::types::task::Task;
use crate::types::task::TaskId;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

pub type JobId = String;

pub enum JobState {
    Pending,
    Ready,
    Running { started_at: Instant },
    Completed { duration: Duration },
    Failed {
        error: String,
        duration: Duration,
    }
}

pub struct Job {
    pub id: JobId,
    pub tasks: HashMap<TaskId, Task>,
    pub state: JobState,
}
