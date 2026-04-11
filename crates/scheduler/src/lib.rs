pub mod drf;
pub mod scheduler_error;

use runtime::task::{TaskId, TaskSpec};

use crate::scheduler_error::SchedulerError;

#[derive(Clone)]
pub struct NodeResources {
    pub node_id: String,
    pub available_cpu: f64,
    pub available_memory_bytes: u64,
    pub available_disk_bytes: u64,
    pub available_gpu: u32,
}

pub struct Assignment {
    pub task_id: TaskId,
    pub node_id: String,
}

pub trait Scheduler {
    fn schedule(
        &self,
        tasks: &[&TaskSpec],
        nodes: &[NodeResources],
    ) -> Result<(Vec<Assignment>, Vec<SchedulerError>), SchedulerError>;
}
