use std::collections::HashMap;
use std::time::Duration;

use agent::orchestrator::Orchestrator;
use runtime::job::{JobSpec, JobState};
use runtime::task::*;
use scheduler::NodeResources;
use scheduler::drf::DrfScheduler;

fn make_task(id: &str, deps: Vec<&str>) -> (TaskId, Task) {
    make_task_with_resources(id, deps, 1.0, 512, 1000)
}

fn make_task_with_resources(
    id: &str,
    deps: Vec<&str>,
    cpu: f64,
    mem: u64,
    disk: u64,
) -> (TaskId, Task) {
    (
        id.to_string(),
        Task::new(TaskSpec {
            id: id.to_string(),
            image: "alpine:latest".to_string(),
            command: vec!["echo".to_string(), "hello".to_string()],
            env: HashMap::new(),
            resources: ResourceRequirements {
                cpu_cores: cpu,
                memory_bytes: mem,
                disk_bytes: disk,
                gpu: false,
            },
            depends_on: deps.into_iter().map(String::from).collect(),
            kind: TaskKind::Batch {
                timeout_s: Duration::from_secs(60),
            },
        }),
    )
}

fn make_job_spec(id: &str, tasks: Vec<(TaskId, Task)>) -> JobSpec {
    JobSpec {
        id: id.to_string(),
        tasks: tasks.into_iter().collect(),
    }
}

fn make_nodes(count: usize) -> Vec<NodeResources> {
    (0..count)
        .map(|i| NodeResources {
            node_id: format!("node-{}", i),
            available_cpu: 4.0,
            available_memory_bytes: 4096,
            available_disk_bytes: 100000,
            available_gpu: 0,
        })
        .collect()
}

fn make_output(exit_code: i32) -> TaskOutput {
    TaskOutput {
        stdout: vec![],
        stderr: vec![],
        exit_code,
    }
}

// ═══════════════════════════════════════════════════════════
// SUBMIT JOB
// ═══════════════════════════════════════════════════════════

#[test]
fn submit_valid_job() {
    let mut orch = Orchestrator::new(DrfScheduler);
    let spec = make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec!["a"])],
    );

    assert!(orch.submit_job(spec).is_ok());
    assert!(orch.jobs.contains_key("job-1"));
    assert!(matches!(orch.jobs["job-1"].state, JobState::Pending));
}

#[test]
fn submit_job_with_cycle_fails() {
    let mut orch = Orchestrator::new(DrfScheduler);
    let spec = make_job_spec(
        "job-1",
        vec![make_task("a", vec!["b"]), make_task("b", vec!["a"])],
    );

    assert!(orch.submit_job(spec).is_err());
    assert!(!orch.jobs.contains_key("job-1"));
}

#[test]
fn submit_job_with_missing_dep_fails() {
    let mut orch = Orchestrator::new(DrfScheduler);
    let spec = make_job_spec("job-1", vec![make_task("a", vec!["nonexistent"])]);

    assert!(orch.submit_job(spec).is_err());
}

#[test]
fn submit_duplicate_job_id_overwrites() {
    let mut orch = Orchestrator::new(DrfScheduler);
    let spec1 = make_job_spec("job-1", vec![make_task("a", vec![])]);
    let spec2 = make_job_spec(
        "job-1",
        vec![make_task("x", vec![]), make_task("y", vec![])],
    );

    orch.submit_job(spec1).unwrap();
    orch.submit_job(spec2).unwrap();

    assert_eq!(orch.jobs["job-1"].spec.tasks.len(), 2);
}

// ═══════════════════════════════════════════════════════════
// GET READY TASKS
// ═══════════════════════════════════════════════════════════

#[test]
fn ready_tasks_returns_roots() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec!["a"])],
    ))
    .unwrap();

    let ready = orch.get_ready_tasks(&"job-1".to_string());
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0], "a");
}

#[test]
fn ready_tasks_returns_empty_for_unknown_job() {
    let orch = Orchestrator::new(DrfScheduler);
    let ready = orch.get_ready_tasks(&"nonexistent".to_string());
    assert!(ready.is_empty());
}

#[test]
fn ready_tasks_all_independent() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![
            make_task("a", vec![]),
            make_task("b", vec![]),
            make_task("c", vec![]),
        ],
    ))
    .unwrap();

    let ready = orch.get_ready_tasks(&"job-1".to_string());
    assert_eq!(ready.len(), 3);
}

// ═══════════════════════════════════════════════════════════
// SCHEDULE READY TASKS
// ═══════════════════════════════════════════════════════════

#[test]
fn schedule_assigns_root_tasks() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec!["a"])],
    ))
    .unwrap();

    let nodes = make_nodes(2);
    let dispatches = orch.schedule_ready_tasks("job-1", &nodes);

    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].0.id, "a");

    // Task a should now be Running
    let job = &orch.jobs["job-1"];
    assert!(matches!(
        job.spec.tasks["a"].state,
        TaskState::Running { .. }
    ));

    // Job should transition to Running
    assert!(matches!(job.state, JobState::Running { .. }));
}

#[test]
fn schedule_does_not_dispatch_blocked_tasks() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec!["a"])],
    ))
    .unwrap();

    let nodes = make_nodes(2);
    let dispatches = orch.schedule_ready_tasks("job-1", &nodes);

    // Only a should be dispatched, b is blocked
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].0.id, "a");
    assert!(matches!(
        orch.jobs["job-1"].spec.tasks["b"].state,
        TaskState::Pending
    ));
}

#[test]
fn schedule_parallel_independent_tasks() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![
            make_task("a", vec![]),
            make_task("b", vec![]),
            make_task("c", vec!["a", "b"]),
        ],
    ))
    .unwrap();

    let nodes = make_nodes(2);
    let dispatches = orch.schedule_ready_tasks("job-1", &nodes);

    assert_eq!(dispatches.len(), 2);
    let ids: Vec<&str> = dispatches.iter().map(|(s, _)| s.id.as_str()).collect();
    assert!(ids.contains(&"a"));
    assert!(ids.contains(&"b"));
}

#[test]
fn schedule_no_nodes_returns_empty() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec("job-1", vec![make_task("a", vec![])]))
        .unwrap();

    let dispatches = orch.schedule_ready_tasks("job-1", &[]);
    assert!(dispatches.is_empty());
}

#[test]
fn schedule_already_running_not_rescheduled() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec("job-1", vec![make_task("a", vec![])]))
        .unwrap();

    let nodes = make_nodes(1);

    // First schedule
    let dispatches1 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(dispatches1.len(), 1);

    // Second schedule — a is Running, nothing new to dispatch
    let dispatches2 = orch.schedule_ready_tasks("job-1", &nodes);
    assert!(dispatches2.is_empty());
}

// ═══════════════════════════════════════════════════════════
// HANDLE TASK RESULT
// ═══════════════════════════════════════════════════════════

#[test]
fn handle_task_result_success() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec("job-1", vec![make_task("a", vec![])]))
        .unwrap();

    // Schedule first to set Running
    let nodes = make_nodes(1);
    orch.schedule_ready_tasks("job-1", &nodes);

    let result =
        orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0));

    assert!(result.is_ok());
    assert!(matches!(
        orch.jobs["job-1"].spec.tasks["a"].state,
        TaskState::Completed { .. }
    ));
}

#[test]
fn handle_task_result_failure() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec("job-1", vec![make_task("a", vec![])]))
        .unwrap();

    let nodes = make_nodes(1);
    orch.schedule_ready_tasks("job-1", &nodes);

    let result = orch.handle_task_result(
        &"job-1".to_string(),
        &"a".to_string(),
        false,
        make_output(1),
    );

    assert!(result.is_ok());
    assert!(matches!(
        orch.jobs["job-1"].spec.tasks["a"].state,
        TaskState::Failed { .. }
    ));
}

#[test]
fn handle_task_result_unknown_job() {
    let mut orch = Orchestrator::new(DrfScheduler);
    let result = orch.handle_task_result(
        &"nonexistent".to_string(),
        &"a".to_string(),
        true,
        make_output(0),
    );
    assert!(result.is_err());
}

// ═══════════════════════════════════════════════════════════
// COMPLETE JOB
// ═══════════════════════════════════════════════════════════

#[test]
fn complete_job_all_tasks_succeeded() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec![])],
    ))
    .unwrap();

    let nodes = make_nodes(2);
    orch.schedule_ready_tasks("job-1", &nodes);

    orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0))
        .unwrap();
    orch.handle_task_result(&"job-1".to_string(), &"b".to_string(), true, make_output(0))
        .unwrap();

    assert!(orch.complete_job(&"job-1".to_string()));
    assert!(matches!(
        orch.jobs["job-1"].state,
        JobState::Completed { .. }
    ));
}

#[test]
fn complete_job_with_failed_task() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec![])],
    ))
    .unwrap();

    let nodes = make_nodes(2);
    orch.schedule_ready_tasks("job-1", &nodes);

    orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0))
        .unwrap();
    orch.handle_task_result(
        &"job-1".to_string(),
        &"b".to_string(),
        false,
        make_output(1),
    )
    .unwrap();

    assert!(orch.complete_job(&"job-1".to_string()));
    assert!(matches!(orch.jobs["job-1"].state, JobState::Failed { .. }));
}

#[test]
fn complete_job_not_all_done_returns_false() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![make_task("a", vec![]), make_task("b", vec!["a"])],
    ))
    .unwrap();

    let nodes = make_nodes(1);
    orch.schedule_ready_tasks("job-1", &nodes);
    orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0))
        .unwrap();

    // b is still Pending, job not complete
    assert!(!orch.complete_job(&"job-1".to_string()));
}

// ═══════════════════════════════════════════════════════════
// FULL DAG FLOW
// ═══════════════════════════════════════════════════════════

#[test]
fn full_linear_dag_flow() {
    // A → B → C
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ],
    ))
    .unwrap();

    let nodes = make_nodes(2);

    // Wave 1: only A is ready
    let d1 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(d1.len(), 1);
    assert_eq!(d1[0].0.id, "a");

    // Complete A → B becomes ready
    orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0))
        .unwrap();

    let d2 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(d2.len(), 1);
    assert_eq!(d2[0].0.id, "b");

    // Complete B → C becomes ready
    orch.handle_task_result(&"job-1".to_string(), &"b".to_string(), true, make_output(0))
        .unwrap();

    let d3 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(d3.len(), 1);
    assert_eq!(d3[0].0.id, "c");

    // Complete C → job done
    orch.handle_task_result(&"job-1".to_string(), &"c".to_string(), true, make_output(0))
        .unwrap();
    assert!(orch.complete_job(&"job-1".to_string()));
    assert!(matches!(
        orch.jobs["job-1"].state,
        JobState::Completed { .. }
    ));
}

#[test]
fn full_diamond_dag_flow() {
    //     A
    //    / \
    //   B   C
    //    \ /
    //     D
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a"]),
            make_task("d", vec!["b", "c"]),
        ],
    ))
    .unwrap();

    let nodes = make_nodes(3);

    // Wave 1: A
    let d1 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(d1.len(), 1);
    assert_eq!(d1[0].0.id, "a");

    orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0))
        .unwrap();

    // Wave 2: B and C in parallel
    let d2 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(d2.len(), 2);
    let ids: Vec<&str> = d2.iter().map(|(s, _)| s.id.as_str()).collect();
    assert!(ids.contains(&"b"));
    assert!(ids.contains(&"c"));

    // Complete B only — D still blocked
    orch.handle_task_result(&"job-1".to_string(), &"b".to_string(), true, make_output(0))
        .unwrap();
    let d3 = orch.schedule_ready_tasks("job-1", &nodes);
    assert!(d3.is_empty(), "D should still be blocked waiting on C");

    // Complete C → D ready
    orch.handle_task_result(&"job-1".to_string(), &"c".to_string(), true, make_output(0))
        .unwrap();
    let d4 = orch.schedule_ready_tasks("job-1", &nodes);
    assert_eq!(d4.len(), 1);
    assert_eq!(d4[0].0.id, "d");

    orch.handle_task_result(&"job-1".to_string(), &"d".to_string(), true, make_output(0))
        .unwrap();
    assert!(orch.complete_job(&"job-1".to_string()));
}

#[test]
fn dag_stops_on_failed_task() {
    // A → B → C, if B fails, C never runs
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec(
        "job-1",
        vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ],
    ))
    .unwrap();

    let nodes = make_nodes(1);

    orch.schedule_ready_tasks("job-1", &nodes);
    orch.handle_task_result(&"job-1".to_string(), &"a".to_string(), true, make_output(0))
        .unwrap();

    orch.schedule_ready_tasks("job-1", &nodes);
    orch.handle_task_result(
        &"job-1".to_string(),
        &"b".to_string(),
        false,
        make_output(1),
    )
    .unwrap();

    // C should never become ready — its dep B failed, not completed
    let d = orch.schedule_ready_tasks("job-1", &nodes);
    assert!(d.is_empty());

    // Job is not fully complete (C is still Pending), but can't make progress
    assert!(!orch.is_job_complete("job-1"));
}

#[test]
fn multiple_jobs_independent() {
    let mut orch = Orchestrator::new(DrfScheduler);
    orch.submit_job(make_job_spec("job-1", vec![make_task("a", vec![])]))
        .unwrap();
    orch.submit_job(make_job_spec("job-2", vec![make_task("x", vec![])]))
        .unwrap();

    let nodes = make_nodes(2);

    let d1 = orch.schedule_ready_tasks("job-1", &nodes);
    let d2 = orch.schedule_ready_tasks("job-2", &nodes);

    assert_eq!(d1.len(), 1);
    assert_eq!(d2.len(), 1);
    assert_eq!(d1[0].0.id, "a");
    assert_eq!(d2[0].0.id, "x");
}
