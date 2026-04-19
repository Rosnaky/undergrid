use agent::orchestrator::Orchestrator;
use scheduler::drf::DrfScheduler;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tonic::Request;

use agent::node::state::NodeState;
use agent::server::node_agent::NodeAgentService;
use mesh::undergrid::node_agent_server::NodeAgent;
use mesh::undergrid::*;

fn test_state(id: &str) -> Arc<RwLock<NodeState>> {
    Arc::new(RwLock::new(NodeState::new(
        id.to_string(),
        "test-host".to_string(),
        "127.0.0.1".to_string(),
        7070,
        Orchestrator::new(DrfScheduler),
    )))
}

#[tokio::test]
async fn ping_returns_node_id() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state);

    let resp = service
        .ping(Request::new(PingRequest {
            from_node_id: "test".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.node_id, "node-1");
    assert!(resp.uptime_secs < 2);
}

#[tokio::test]
async fn register_adds_peer() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service
        .register(Request::new(RegisterRequest {
            node_info: Some(NodeInfo {
                node_id: "joiner-1".to_string(),
                hostname: "host-2".to_string(),
                ip_address: "127.0.0.1".to_string(),
                port: 7071,
                resources: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.accepted);

    let s = state.read().await;
    assert!(s.raft.is_peer_by_node_id("joiner-1"));
}

#[tokio::test]
async fn register_returns_peer_list() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    // Add first peer
    service
        .register(Request::new(RegisterRequest {
            node_info: Some(NodeInfo {
                node_id: "node-2".to_string(),
                hostname: "host-2".to_string(),
                ip_address: "127.0.0.1".to_string(),
                port: 7071,
                resources: None,
            }),
        }))
        .await
        .unwrap();

    // Add second peer — should get first peer in response
    let resp = service
        .register(Request::new(RegisterRequest {
            node_info: Some(NodeInfo {
                node_id: "node-3".to_string(),
                hostname: "host-3".to_string(),
                ip_address: "127.0.0.1".to_string(),
                port: 7072,
                resources: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    let peer_ids: Vec<String> = resp.peers.iter().map(|p| p.node_id.clone()).collect();
    assert!(peer_ids.contains(&"node-2".to_string()));
}

#[tokio::test]
async fn register_rejects_missing_node_info() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state);

    let result = service
        .register(Request::new(RegisterRequest { node_info: None }))
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn vote_grants_for_higher_term() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state);

    let resp = service
        .vote(Request::new(VoteRequest {
            candidate_id: "node-2".to_string(),
            term: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.granted);
    assert_eq!(resp.term, 1);
}

#[tokio::test]
async fn vote_rejects_stale_term() {
    let state = test_state("node-1");
    {
        let mut s = state.write().await;
        s.raft.term = 5;
    }
    let service = NodeAgentService::new(state);

    let resp = service
        .vote(Request::new(VoteRequest {
            candidate_id: "node-2".to_string(),
            term: 3,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.granted);
    assert_eq!(resp.term, 5);
}

#[tokio::test]
async fn append_entries_accepted() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service
        .append_entries(Request::new(AppendEntriesRequest {
            leader_id: "leader-1".to_string(),
            term: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    let s = state.read().await;
    assert_eq!(s.raft.leader_id, Some("leader-1".to_string()));
}

#[tokio::test]
async fn append_entries_rejected_stale_term() {
    let state = test_state("node-1");
    {
        let mut s = state.write().await;
        s.raft.term = 5;
    }
    let service = NodeAgentService::new(state);

    let resp = service
        .append_entries(Request::new(AppendEntriesRequest {
            leader_id: "old-leader".to_string(),
            term: 3,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
}

#[tokio::test]
async fn add_peer_works() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service
        .add_peer(Request::new(AddPeerRequest {
            peer_node_info: Some(NodeInfo {
                node_id: "node-2".to_string(),
                hostname: "host-2".to_string(),
                ip_address: "127.0.0.1".to_string(),
                port: 7071,
                resources: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    let s = state.read().await;
    assert!(s.raft.is_peer_by_node_id("node-2"));
}

#[tokio::test]
async fn remove_peer_works() {
    let state = test_state("node-1");
    {
        let mut s = state.write().await;
        s.raft.add_peer(raft::Peer {
            node_id: "node-2".to_string(),
            hostname: "host-2".to_string(),
            ip_address: "127.0.0.1".to_string(),
            port: 7071,
            last_seen: Instant::now(),
            status: raft::Status::Operational,
        });
    }
    let service = NodeAgentService::new(state.clone());

    let resp = service
        .remove_peer(Request::new(RemovePeerRequest {
            peer_node_id: "node-2".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    let s = state.read().await;
    assert!(!s.raft.is_peer_by_node_id("node-2"));
}

fn make_proto_task(id: &str, deps: Vec<&str>) -> TaskSpec {
    TaskSpec {
        id: id.to_string(),
        image: "alpine:latest".to_string(),
        command: vec!["echo".to_string(), "hello".to_string()],
        env: HashMap::new(),
        cpu_cores: 1.0,
        memory_bytes: 512,
        disk_bytes: 1000,
        gpu: false,
        depends_on: deps.into_iter().map(String::from).collect(),
        kind: Some(task_spec::Kind::Batch(Batch { timeout_s: 60 })),
    }
}

#[tokio::test]
async fn submit_job_accepted() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    let resp = service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![
                make_proto_task("a", vec![]),
                make_proto_task("b", vec!["a"]),
            ],
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.accepted);
    assert!(resp.error.is_empty());

    let s = state.read().await;
    assert!(s.orchestrator.jobs.contains_key("job-1"));
}

#[tokio::test]
async fn submit_job_rejected_cycle() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![
                make_proto_task("a", vec!["b"]),
                make_proto_task("b", vec!["a"]),
            ],
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.accepted);
    assert!(!resp.error.is_empty());
}

#[tokio::test]
async fn submit_job_rejected_missing_dep() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec!["nonexistent"])],
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.accepted);
}

// ═══════════════════════════════════════════════════════════
// REPORT TASK RESULT
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn report_task_result_updates_state() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    // Submit a job first
    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![
                make_proto_task("a", vec![]),
                make_proto_task("b", vec!["a"]),
            ],
        }))
        .await
        .unwrap();

    // Manually set task a to Running so report_task_result works
    {
        let mut s = state.write().await;
        if let Some(job) = s.orchestrator.jobs.get_mut("job-1") {
            if let Some(task) = job.spec.tasks.get_mut("a") {
                task.state = runtime::task::TaskState::Running {
                    node_id: "leader-1".to_string(),
                    started_at: std::time::Instant::now(),
                };
            }
            job.state = runtime::job::JobState::Running {
                started_at: std::time::Instant::now(),
            };
        }
    }

    // Report success
    let resp = service
        .report_task_result(Request::new(ReportTaskResultRequest {
            node_id: "leader-1".to_string(),
            job_id: "job-1".to_string(),
            task_id: "a".to_string(),
            output: Some(TaskOutput {
                stdout: b"done".to_vec(),
                stderr: vec![],
                exit_code: 0,
            }),
            executed: true,
            error: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.acknowledged);

    // Task a should be Completed
    let s = state.read().await;
    let job = &s.orchestrator.jobs["job-1"];
    assert!(matches!(
        job.spec.tasks["a"].state,
        runtime::task::TaskState::Completed { .. }
    ));
}

#[tokio::test]
async fn report_task_result_completes_job() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    // Submit single-task job
    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec![])],
        }))
        .await
        .unwrap();

    // Set task to Running and job to Running
    {
        let mut s = state.write().await;
        if let Some(job) = s.orchestrator.jobs.get_mut("job-1") {
            if let Some(task) = job.spec.tasks.get_mut("a") {
                task.state = runtime::task::TaskState::Running {
                    node_id: "leader-1".to_string(),
                    started_at: std::time::Instant::now(),
                };
            }
            job.state = runtime::job::JobState::Running {
                started_at: std::time::Instant::now(),
            };
        }
    }

    // Report success
    service
        .report_task_result(Request::new(ReportTaskResultRequest {
            node_id: "leader-id".to_string(),
            job_id: "job-1".to_string(),
            task_id: "a".to_string(),
            output: Some(TaskOutput {
                stdout: vec![],
                stderr: vec![],
                exit_code: 0,
            }),
            executed: true,
            error: String::new(),
        }))
        .await
        .unwrap();

    // Job should be Completed
    let s = state.read().await;
    assert!(matches!(
        s.orchestrator.jobs["job-1"].state,
        runtime::job::JobState::Completed { .. }
    ));
}

#[tokio::test]
async fn report_failed_task_marks_job_failed() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec![])],
        }))
        .await
        .unwrap();

    {
        let mut s = state.write().await;
        if let Some(job) = s.orchestrator.jobs.get_mut("job-1") {
            if let Some(task) = job.spec.tasks.get_mut("a") {
                task.state = runtime::task::TaskState::Running {
                    node_id: "leader-1".to_string(),
                    started_at: std::time::Instant::now(),
                };
            }
            job.state = runtime::job::JobState::Running {
                started_at: std::time::Instant::now(),
            };
        }
    }

    service
        .report_task_result(Request::new(ReportTaskResultRequest {
            node_id: "leader-id".to_string(),
            job_id: "job-1".to_string(),
            task_id: "a".to_string(),
            output: Some(TaskOutput {
                stdout: vec![],
                stderr: vec![],
                exit_code: 1,
            }),
            executed: true,
            error: "segfault".to_string(),
        }))
        .await
        .unwrap();

    let s = state.read().await;
    assert!(matches!(
        s.orchestrator.jobs["job-1"].state,
        runtime::job::JobState::Failed { .. }
    ));
}

#[tokio::test]
async fn get_job_status_returns_not_found_for_missing_job() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    let resp = service
        .get_job_status(Request::new(GetJobStatusRequest {
            job_id: "nonexistent".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.found);
}

#[tokio::test]
async fn get_job_status_returns_pending_after_submit() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec![])],
        }))
        .await
        .unwrap();

    let resp = service
        .get_job_status(Request::new(GetJobStatusRequest {
            job_id: "job-1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found);
    assert_eq!(resp.status, JobStatus::Pending as i32);
}

#[tokio::test]
async fn get_job_status_returns_running() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec![])],
        }))
        .await
        .unwrap();

    {
        let mut s = state.write().await;
        if let Some(job) = s.orchestrator.jobs.get_mut("job-1") {
            job.state = runtime::job::JobState::Running {
                started_at: std::time::Instant::now(),
            };
        }
    }

    let resp = service
        .get_job_status(Request::new(GetJobStatusRequest {
            job_id: "job-1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found);
    assert_eq!(resp.status, JobStatus::Running as i32);
    assert!(resp.elapsed_ms < 1000); // just submitted, should be near zero
}

#[tokio::test]
async fn get_job_status_returns_completed() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec![])],
        }))
        .await
        .unwrap();

    {
        let mut s = state.write().await;
        if let Some(job) = s.orchestrator.jobs.get_mut("job-1") {
            job.state = runtime::job::JobState::Completed {
                duration: std::time::Duration::from_secs(5),
            };
        }
    }

    let resp = service
        .get_job_status(Request::new(GetJobStatusRequest {
            job_id: "job-1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found);
    assert_eq!(resp.status, JobStatus::Completed as i32);
    assert_eq!(resp.elapsed_ms, 5000);
    assert!(resp.error.is_empty());
}

#[tokio::test]
async fn get_job_status_returns_failed_with_error() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    {
        let mut s = state.write().await;
        s.raft.role = raft::Role::Leader;
    }

    service
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-1".to_string(),
            tasks: vec![make_proto_task("a", vec![])],
        }))
        .await
        .unwrap();

    {
        let mut s = state.write().await;
        if let Some(job) = s.orchestrator.jobs.get_mut("job-1") {
            job.state = runtime::job::JobState::Failed {
                error: "out of memory".to_string(),
                duration: std::time::Duration::from_secs(3),
            };
        }
    }

    let resp = service
        .get_job_status(Request::new(GetJobStatusRequest {
            job_id: "job-1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.found);
    assert_eq!(resp.status, JobStatus::Failed as i32);
    assert_eq!(resp.elapsed_ms, 3000);
    assert_eq!(resp.error, "out of memory");
}
