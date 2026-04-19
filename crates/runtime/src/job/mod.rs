pub mod job_error;

use crate::job::job_error::JobError;
use crate::task::Task;
use crate::task::TaskId;
use crate::task::TaskState;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

pub type JobId = String;

#[derive(Clone)]
pub enum JobState {
    Pending,
    Running { started_at: Instant },
    Completed { duration: Duration },
    Failed { error: String, duration: Duration },
}

pub struct JobSpec {
    pub id: JobId,
    pub tasks: HashMap<TaskId, Task>,
}

pub struct Job {
    pub spec: JobSpec,
    pub state: JobState,
}

impl JobSpec {
    /// Uses Kahn's algorithm to create topological sort
    pub fn validate(&self) -> Result<Vec<Vec<TaskId>>, JobError> {
        let mut queue = VecDeque::new();
        let mut dep_to_task_id: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        let mut task_id_to_indegree: HashMap<TaskId, u64> = HashMap::new();

        for (k, v) in &self.tasks {
            // Update indegree
            task_id_to_indegree.insert(k.to_string(), v.spec.depends_on.len() as u64);

            // Update adj matrix
            for dep in &v.spec.depends_on {
                if !self.tasks.contains_key(dep) {
                    return Err(JobError::MissingDependency(k.clone(), dep.clone()));
                }
                dep_to_task_id
                    .entry(dep.clone())
                    .or_default()
                    .push(k.clone());
            }
        }

        for (k, v) in &task_id_to_indegree {
            if *v == 0 {
                queue.push_back(k.clone());
            }
        }

        let mut ans: Vec<Vec<TaskId>> = Vec::new();
        while !queue.is_empty() {
            let mut curr: Vec<TaskId> = Vec::new();
            let s = queue.len();

            for _ in 0..s {
                let id = queue.pop_front().unwrap();

                if let Some(dependents) = dep_to_task_id.get(&id) {
                    for dep in dependents {
                        let count = task_id_to_indegree.get_mut(dep).unwrap();
                        *count -= 1;
                        if *count == 0 {
                            queue.push_back(dep.clone());
                        }
                    }
                }
                curr.push(id.clone());
            }
            ans.push(curr);
        }

        let processed: usize = ans.iter().map(|level| level.len()).sum();
        if processed != self.tasks.len() {
            return Err(JobError::CycleDetected);
        }

        Ok(ans)
    }
}

impl Job {
    pub fn get_ready_tasks(&self) -> Vec<&TaskId> {
        self.spec
            .tasks
            .iter()
            .filter(|(_, task)| {
                matches!(task.state, TaskState::Pending)
                    && task.spec.depends_on.iter().all(|dep_id| {
                        matches!(
                            self.spec.tasks.get(dep_id).map(|t| &t.state),
                            Some(TaskState::Completed { .. })
                        )
                    })
            })
            .map(|(id, _)| id)
            .collect()
    }
}
