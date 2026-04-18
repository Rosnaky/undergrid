use std::time::Duration;

use crate::undergrid::{
    Batch, PortMapping as ProtoPortMapping, RestartConfig, RestartPolicy as ProtoRestartPolicy,
    Service, TaskOutput as ProtoTaskOutput, TaskSpec as ProtoTaskSpec, task_spec,
};
use runtime::task::{
    PortMapping, Protocol, ResourceRequirements, RestartPolicy, TaskKind,
    TaskOutput as RuntimeTaskOutput, TaskSpec as RuntimeTaskSpec,
};

impl TryFrom<RestartConfig> for RestartPolicy {
    type Error = String;

    fn try_from(config: RestartConfig) -> Result<Self, Self::Error> {
        let policy = ProtoRestartPolicy::try_from(config.policy)
            .map_err(|_| "Invalid restart policy".to_string())?;

        match policy {
            ProtoRestartPolicy::Always => Ok(RestartPolicy::Always),
            ProtoRestartPolicy::Retries => Ok(RestartPolicy::Retries {
                max_retries: config.max_retries,
                retry_delay_s: config.retry_delay_s,
            }),
            ProtoRestartPolicy::None => Ok(RestartPolicy::None),
        }
    }
}

impl From<RestartPolicy> for RestartConfig {
    fn from(policy: RestartPolicy) -> Self {
        match policy {
            RestartPolicy::Always => RestartConfig {
                policy: ProtoRestartPolicy::Always as i32,
                max_retries: 0,
                retry_delay_s: 0,
            },
            RestartPolicy::Retries {
                max_retries,
                retry_delay_s,
            } => RestartConfig {
                policy: ProtoRestartPolicy::Retries as i32,
                max_retries,
                retry_delay_s,
            },
            RestartPolicy::None => RestartConfig {
                policy: ProtoRestartPolicy::None as i32,
                max_retries: 0,
                retry_delay_s: 0,
            },
        }
    }
}

impl TryFrom<ProtoTaskSpec> for RuntimeTaskSpec {
    type Error = String;

    fn try_from(proto: ProtoTaskSpec) -> Result<Self, Self::Error> {
        let kind = match proto.kind {
            Some(task_spec::Kind::Batch(b)) => TaskKind::Batch {
                timeout_s: Duration::from_secs(b.timeout_s),
            },
            Some(task_spec::Kind::Service(s)) => {
                let restart_config = s.restart_config.ok_or("Missing restart config")?;
                TaskKind::Service {
                    health_check: if s.health_check.is_empty() {
                        None
                    } else {
                        Some(s.health_check)
                    },
                    restart_policy: RestartPolicy::try_from(restart_config)?,
                    port: s
                        .ports
                        .into_iter()
                        .map(|p| PortMapping {
                            container_port: p.container_port as u16,
                            protocol: match p.protocol.as_str() {
                                "udp" => Protocol::UDP,
                                _ => Protocol::TCP,
                            },
                        })
                        .collect(),
                }
            }
            None => return Err("Missing task kind".to_string()),
        };

        Ok(RuntimeTaskSpec {
            id: proto.id,
            image: proto.image,
            command: proto.command,
            env: proto.env,
            resources: ResourceRequirements {
                cpu_cores: proto.cpu_cores,
                memory_bytes: proto.memory_bytes,
                disk_bytes: proto.disk_bytes,
                gpu: proto.gpu,
            },
            depends_on: proto.depends_on,
            kind,
        })
    }
}

impl From<RuntimeTaskSpec> for ProtoTaskSpec {
    fn from(spec: RuntimeTaskSpec) -> Self {
        let kind = match spec.kind {
            TaskKind::Batch { timeout_s } => task_spec::Kind::Batch(Batch {
                timeout_s: timeout_s.as_secs(),
            }),
            TaskKind::Service {
                health_check,
                restart_policy,
                port,
            } => task_spec::Kind::Service(Service {
                health_check: health_check.unwrap_or_default(),
                restart_config: Some(RestartConfig::from(restart_policy)),
                ports: port
                    .into_iter()
                    .map(|p| ProtoPortMapping {
                        container_port: p.container_port as u32,
                        protocol: match p.protocol {
                            Protocol::UDP => "udp".to_string(),
                            Protocol::TCP => "tcp".to_string(),
                        },
                    })
                    .collect(),
            }),
        };

        ProtoTaskSpec {
            id: spec.id,
            image: spec.image,
            command: spec.command,
            env: spec.env,
            cpu_cores: spec.resources.cpu_cores,
            memory_bytes: spec.resources.memory_bytes,
            disk_bytes: spec.resources.disk_bytes,
            gpu: spec.resources.gpu,
            depends_on: spec.depends_on,
            kind: Some(kind),
        }
    }
}

impl From<ProtoTaskOutput> for RuntimeTaskOutput {
    fn from(proto: ProtoTaskOutput) -> Self {
        RuntimeTaskOutput {
            stdout: proto.stdout,
            stderr: proto.stderr,
            exit_code: proto.exit_code,
        }
    }
}

impl From<RuntimeTaskOutput> for ProtoTaskOutput {
    fn from(output: RuntimeTaskOutput) -> Self {
        ProtoTaskOutput {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_code: output.exit_code,
        }
    }
}
