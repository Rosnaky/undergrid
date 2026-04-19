use crate::undergrid::{GetJobStatusResponse, JobStatus as ProtoJobStatus};
use runtime::job::JobState;

impl From<Option<JobState>> for GetJobStatusResponse {
    fn from(state: Option<JobState>) -> Self {
        match state {
            None => GetJobStatusResponse {
                found: false,
                status: ProtoJobStatus::Pending as i32,
                elapsed_ms: 0,
                error: String::new(),
            },
            Some(JobState::Pending) => GetJobStatusResponse {
                found: true,
                status: ProtoJobStatus::Pending as i32,
                elapsed_ms: 0,
                error: String::new(),
            },
            Some(JobState::Running { started_at }) => GetJobStatusResponse {
                found: true,
                status: ProtoJobStatus::Running as i32,
                elapsed_ms: started_at.elapsed().as_millis() as u64,
                error: String::new(),
            },
            Some(JobState::Completed { duration }) => GetJobStatusResponse {
                found: true,
                status: ProtoJobStatus::Completed as i32,
                elapsed_ms: duration.as_millis() as u64,
                error: String::new(),
            },
            Some(JobState::Failed { error, duration }) => GetJobStatusResponse {
                found: true,
                status: ProtoJobStatus::Failed as i32,
                elapsed_ms: duration.as_millis() as u64,
                error,
            },
        }
    }
}
