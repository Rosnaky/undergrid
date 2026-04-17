// crates/cli/src/job.rs

use mesh::undergrid::*;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct JobFile {
    pub job: JobMeta,
    pub tasks: HashMap<String, TaskDef>,
}

#[derive(Deserialize)]
pub struct JobMeta {
    pub id: String,
}

#[derive(Deserialize)]
pub struct TaskDef {
    pub image: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub cpu_cores: f64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    #[serde(default)]
    pub gpu: bool,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
}

fn default_timeout() -> u64 {
    60
}

impl TaskDef {
    pub fn to_proto(&self, id: &str) -> TaskSpec {
        TaskSpec {
            id: id.to_string(),
            image: self.image.clone(),
            command: self.command.clone(),
            env: self.env.clone(),
            cpu_cores: self.cpu_cores,
            memory_bytes: self.memory_bytes,
            disk_bytes: self.disk_bytes,
            gpu: self.gpu,
            depends_on: self.depends_on.clone(),
            kind: Some(task_spec::Kind::Batch(Batch {
                timeout_s: self.timeout_s,
            })),
        }
    }
}
