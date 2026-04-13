use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::task::task_error::TaskError;
pub mod task_error;

pub type TaskId = String;
pub type HealthCheck = String;

#[derive(Clone)]
pub enum TaskKind {
    /// Run to completion
    Batch { timeout_s: Duration },
    Service {
        /// Health check endpoint
        health_check: Option<HealthCheck>,
        restart_policy: RestartPolicy,
        /// Ports to expose
        port: Vec<PortMapping>,
    },
}

#[derive(Clone)]
pub enum Protocol {
    UDP,
    TCP,
}

#[derive(Default, Clone)]
pub struct TaskOutput {
    pub stdout: Vec<u8>,
    pub exit_code: i32,
}

#[derive(Clone)]
pub enum RestartPolicy {
    Always,
    Retries {
        /// Maximum retries
        max_retries: u64,
        retry_delay_s: u64,
    },
    None,
}

#[derive(Clone)]
pub struct PortMapping {
    pub container_port: u16,
    pub protocol: Protocol,
}

#[derive(Clone)]
pub struct ResourceRequirements {
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub cpu_cores: f64,
    pub gpu: bool,
}

pub enum TaskState {
    Pending,
    Running {
        node_id: String,
        started_at: Instant,
    },
    Completed {
        output: TaskOutput,
        duration: Duration,
    },
    Failed {
        error: String,
        duration: Duration,
    },
}

#[derive(Clone)]
pub struct TaskSpec {
    pub id: TaskId,
    /// Docker image
    pub image: String,
    /// Commands to run
    pub command: Vec<String>,
    /// Environment variables
    pub env: HashMap<String, String>,
    pub resources: ResourceRequirements,
    pub depends_on: Vec<TaskId>,
    /// Kind of task
    pub kind: TaskKind,
}

pub struct Task {
    pub spec: TaskSpec,
    pub state: TaskState,
}

impl Task {
    pub fn new(spec: TaskSpec) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
        }
    }

    pub fn complete_task(&mut self, output: TaskOutput) -> Result<(), TaskError> {
        let duration = match &self.state {
            TaskState::Running { started_at, .. } => started_at.elapsed(),
            _ => {
                return Err(TaskError::InvalidStateTransition(
                    "Task is currently running".to_string(),
                ));
            }
        };

        self.state = TaskState::Completed { output, duration };
        Ok(())
    }
}
