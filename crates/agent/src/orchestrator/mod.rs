pub mod orchestrator_error;

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use runtime::{
    job::{Job, JobId, JobSpec, JobState},
    task::{TaskId, TaskOutput, TaskSpec, TaskState},
};
use scheduler::{NodeResources, Scheduler};

use crate::orchestrator::orchestrator_error::OrchestratorError;

pub struct Orchestrator {
    pub jobs: HashMap<JobId, Job>,
    scheduler: Box<dyn Scheduler + Send + Sync>,
}

impl Orchestrator {
    pub fn new<S: Scheduler + Send + Sync + 'static>(scheduler: S) -> Self {
        Self {
            jobs: HashMap::new(),
            scheduler: Box::new(scheduler),
        }
    }

    pub fn submit_job(&mut self, job_spec: JobSpec) -> Result<(), OrchestratorError> {
        job_spec
            .validate()
            .map_err(|e| OrchestratorError::ValidationError(e.to_string()))?;

        let job_id = job_spec.id.clone();
        self.jobs.insert(
            job_id,
            Job {
                spec: job_spec,
                state: JobState::Pending,
            },
        );
        Ok(())
    }

    pub fn get_ready_tasks(&self, job_id: &JobId) -> Vec<TaskId> {
        let job = match self.jobs.get(job_id) {
            Some(j) => j,
            None => return vec![],
        };

        job.spec
            .tasks
            .iter()
            .filter(|(_, task)| {
                matches!(task.state, TaskState::Pending)
                    && task.spec.depends_on.iter().all(|dep| {
                        job.spec
                            .tasks
                            .get(dep)
                            .map(|t| matches!(t.state, TaskState::Completed { .. }))
                            .unwrap_or(false)
                    })
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn schedule_ready_tasks(
        &mut self,
        job_id: &str,
        nodes: &[NodeResources],
    ) -> Vec<(TaskSpec, String)> {
        let job = match self.jobs.get(job_id) {
            Some(j) => j,
            None => return vec![],
        };

        let ready_ids = job.get_ready_tasks();
        if ready_ids.is_empty() {
            return vec![];
        }

        let ready_task_specs: Vec<&TaskSpec> = ready_ids
            .iter()
            .filter_map(|id| job.spec.tasks.get(*id).map(|t| &t.spec))
            .collect();

        let (assignments, _errors) = self
            .scheduler
            .schedule(&ready_task_specs, nodes)
            .unwrap_or_default();

        if assignments.is_empty() {
            return vec![];
        }

        let job = self.jobs.get_mut(job_id).unwrap();
        let mut to_dispatch = Vec::new();

        for assignment in assignments {
            if let Some(task) = job.spec.tasks.get_mut(&assignment.task_id) {
                task.state = TaskState::Running {
                    node_id: assignment.node_id.clone(),
                    started_at: Instant::now(),
                };
                to_dispatch.push((task.spec.clone(), assignment.node_id));
            }
        }

        if matches!(job.state, JobState::Pending) {
            job.state = JobState::Running {
                started_at: Instant::now(),
            };
        }

        to_dispatch
    }

    pub fn handle_task_result(
        &mut self,
        job_id: &JobId,
        task_id: &TaskId,
        success: bool,
        output: TaskOutput,
    ) -> Result<(), OrchestratorError> {
        let job = match self.jobs.get_mut(job_id) {
            Some(j) => j,
            None => {
                return Err(OrchestratorError::ValidationError(format!(
                    "No job exists with job_id: {}",
                    job_id
                )));
            }
        };

        if let Some(task) = job.spec.tasks.get_mut(task_id) {
            let duration = match &task.state {
                TaskState::Running { started_at, .. } => started_at.elapsed(),
                _ => Duration::from_secs(0),
            };

            task.state = if success {
                TaskState::Completed { output, duration }
            } else {
                TaskState::Failed {
                    error: "Task failed".to_string(),
                    duration,
                }
            }
        }

        Ok(())
    }

    /*
    @return bool: true if the job is marked as fully completed
    */
    pub fn complete_job(&mut self, job_id: &JobId) -> bool {
        if !self.is_job_complete(job_id) {
            return false;
        }

        let job = match self.jobs.get_mut(job_id) {
            Some(job) => job,
            None => return false,
        };

        let started_at = match &job.state {
            JobState::Running { started_at } => *started_at,
            _ => return false,
        };

        let tasks_failed = job
            .spec
            .tasks
            .values()
            .any(|task| matches!(task.state, TaskState::Failed { .. }));

        let duration = started_at.elapsed();

        job.state = if tasks_failed {
            JobState::Failed {
                error: "One or more tasks failed".to_string(),
                duration,
            }
        } else {
            JobState::Completed { duration }
        };

        true
    }

    pub fn is_job_complete(&self, job_id: &str) -> bool {
        self.jobs.get(job_id).is_some_and(|job| {
            job.spec.tasks.values().all(|t| {
                matches!(
                    t.state,
                    TaskState::Completed { .. } | TaskState::Failed { .. }
                )
            })
        })
    }
}
