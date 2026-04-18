pub mod executor_error;

use std::{
    process::Stdio,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::process::Command;

use crate::{
    executor::executor_error::ExecutorError,
    task::{TaskKind, TaskOutput, TaskSpec},
};

pub struct Executor;

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute(&self, spec: &TaskSpec) -> Result<TaskOutput, ExecutorError> {
        self.pull_image(&spec.image).await?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let container_name = format!("undergrid-{}-{}", spec.id, timestamp);
        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("--cpus")
            .arg(spec.resources.cpu_cores.to_string())
            .arg("--memory")
            .arg(format!("{}m", spec.resources.memory_bytes / 1_048_576))
            .arg("--name")
            .arg(&container_name);

        // Mode-specific flags
        match &spec.kind {
            TaskKind::Batch { .. } => {
                cmd.arg("--rm");
            }
            TaskKind::Service { .. } => {
                cmd.arg("-d");
            }
        }

        if let TaskKind::Service { port, .. } = &spec.kind {
            for mapping in port {
                cmd.arg("-p").arg(format!(
                    "{}:{}",
                    mapping.container_port, mapping.container_port
                ));
            }
        }

        for (key, value) in &spec.env {
            cmd.arg("--env");
            cmd.arg(format!("{}={}", key, value));
        }

        // Image and command
        cmd.arg(&spec.image);
        for c in &spec.command {
            cmd.arg(c);
        }

        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        match &spec.kind {
            TaskKind::Batch { timeout_s: timeout } => {
                match tokio::time::timeout(*timeout, cmd.output()).await {
                    Ok(result) => {
                        let output =
                            result.map_err(|e| ExecutorError::ExecutionFailed(e.to_string()))?;
                        Ok(TaskOutput {
                            stdout: output.stdout,
                            stderr: output.stderr,
                            exit_code: output.status.code().unwrap_or(-1),
                        })
                    }
                    Err(_) => {
                        let _ = Command::new("docker")
                            .arg("kill")
                            .arg(&container_name)
                            .output()
                            .await;
                        Err(ExecutorError::Timeout)
                    }
                }
            }
            TaskKind::Service { .. } => {
                let output = cmd
                    .output()
                    .await
                    .map_err(|e| ExecutorError::ExecutionFailed(e.to_string()))?;

                if !output.status.success() {
                    return Err(ExecutorError::ExecutionFailed(
                        String::from_utf8_lossy(&output.stderr).to_string(),
                    ));
                }

                Ok(TaskOutput {
                    stdout: container_name.into_bytes(),
                    stderr: vec![],
                    exit_code: 0,
                })
            }
        }
    }

    pub async fn pull_image(&self, image: &str) -> Result<(), ExecutorError> {
        let output = Command::new("docker")
            .arg("pull")
            .arg(image)
            .output()
            .await
            .map_err(|e| ExecutorError::ExecutionFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(ExecutorError::ImagePullFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        Ok(())
    }
}
