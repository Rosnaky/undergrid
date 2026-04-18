use runtime::job::Job;
use runtime::job::JobSpec;
use runtime::job::JobState;
use runtime::job::job_error::JobError;
use runtime::task::ResourceRequirements;
use runtime::task::Task;
use runtime::task::TaskId;
use runtime::task::TaskKind;
use runtime::task::TaskOutput;
use runtime::task::TaskSpec;
use runtime::task::TaskState;
use std::collections::HashMap;
use std::time::Duration;

fn make_task(id: &str, deps: Vec<&str>) -> (TaskId, Task) {
    (
        id.to_string(),
        Task {
            spec: TaskSpec {
                id: id.to_string(),
                image: "test:latest".to_string(),
                command: vec![],
                env: HashMap::new(),
                resources: ResourceRequirements {
                    memory_bytes: 0,
                    disk_bytes: 0,
                    cpu_cores: 1.0,
                    gpu: false,
                },
                depends_on: deps.into_iter().map(String::from).collect(),
                kind: TaskKind::Batch {
                    timeout_s: Duration::from_secs(60),
                },
            },
            state: TaskState::Pending,
        },
    )
}

fn make_job(tasks: Vec<(TaskId, Task)>) -> Job {
    Job {
        spec: JobSpec {
            id: "test-job".to_string(),
            tasks: tasks.into_iter().collect(),
        },
        state: JobState::Pending,
    }
}

// ---- validate() tests ----

#[test]
fn validate_passes_on_linear_graph() {
    // A → B → C
    let job = make_job(vec![
        make_task("a", vec![]),
        make_task("b", vec!["a"]),
        make_task("c", vec!["b"]),
    ]);

    let levels = job.spec.validate().unwrap();
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0], vec!["a"]);
    assert_eq!(levels[1], vec!["b"]);
    assert_eq!(levels[2], vec!["c"]);
}

#[test]
fn validate_passes_on_diamond_graph() {
    // A → B, C → D
    let job = make_job(vec![
        make_task("a", vec![]),
        make_task("b", vec!["a"]),
        make_task("c", vec!["a"]),
        make_task("d", vec!["b", "c"]),
    ]);

    let levels = job.spec.validate().unwrap();
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0], vec!["a"]);
    assert!(levels[1].contains(&"b".to_string()));
    assert!(levels[1].contains(&"c".to_string()));
    assert_eq!(levels[1].len(), 2);
    assert_eq!(levels[2], vec!["d"]);
}

#[test]
fn validate_independent_tasks_single_level() {
    let job = make_job(vec![
        make_task("a", vec![]),
        make_task("b", vec![]),
        make_task("c", vec![]),
    ]);

    let levels = job.spec.validate().unwrap();
    assert_eq!(levels.len(), 1);
    assert_eq!(levels[0].len(), 3);
}

#[test]
fn validate_detects_cycle() {
    let job = make_job(vec![make_task("a", vec!["b"]), make_task("b", vec!["a"])]);

    let result = job.spec.validate();
    assert!(matches!(result, Err(JobError::CycleDetected)));
}

#[test]
fn validate_detects_missing_dependency() {
    let job = make_job(vec![make_task("a", vec!["nonexistent"])]);

    let result = job.spec.validate();
    assert!(matches!(result, Err(JobError::MissingDependency(_, _))));
}

#[test]
fn validate_empty_job() {
    let job = make_job(vec![]);
    let levels = job.spec.validate().unwrap();
    assert!(levels.is_empty());
}

// ---- get_ready_tasks() tests ----

#[test]
fn ready_tasks_returns_roots_initially() {
    let job = make_job(vec![make_task("a", vec![]), make_task("b", vec!["a"])]);

    let ready = job.get_ready_tasks();
    // a has no deps and is Pending → ready
    // b depends on a which is Pending → not ready
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0], "a");
}

#[test]
fn ready_tasks_unblocks_after_completion() {
    let mut job = make_job(vec![
        make_task("a", vec![]),
        make_task("b", vec!["a"]),
        make_task("c", vec!["a"]),
    ]);

    // Complete task a
    job.spec
        .tasks
        .get_mut("a")
        .unwrap()
        .complete_task(TaskOutput {
            stdout: vec![],
            stderr: vec![],
            exit_code: 0,
        })
        .ok(); // will error since not Running, so set state manually:

    job.spec.tasks.get_mut("a").unwrap().state = TaskState::Completed {
        output: TaskOutput {
            stdout: vec![],
            stderr: vec![],
            exit_code: 0,
        },
        duration: Duration::from_secs(1),
    };

    let ready = job.get_ready_tasks();
    assert_eq!(ready.len(), 2);
    assert!(ready.contains(&&"b".to_string()));
    assert!(ready.contains(&&"c".to_string()));
}

#[test]
fn ready_tasks_blocked_when_dep_not_completed() {
    let mut job = make_job(vec![make_task("a", vec![]), make_task("b", vec!["a"])]);

    // a is Running, not Completed
    job.spec.tasks.get_mut("a").unwrap().state = TaskState::Running {
        node_id: "node-1".to_string(),
        started_at: std::time::Instant::now(),
    };

    let ready = job.get_ready_tasks();
    assert!(ready.is_empty());
}

#[test]
fn ready_tasks_partial_deps_completed() {
    // D depends on both B and C
    let mut job = make_job(vec![
        make_task("a", vec![]),
        make_task("b", vec!["a"]),
        make_task("c", vec!["a"]),
        make_task("d", vec!["b", "c"]),
    ]);

    // Complete a and b, but not c
    for id in &["a", "b"] {
        job.spec.tasks.get_mut(*id).unwrap().state = TaskState::Completed {
            output: TaskOutput {
                stdout: vec![],
                stderr: vec![],
                exit_code: 0,
            },
            duration: Duration::from_secs(1),
        };
    }

    let ready = job.get_ready_tasks();
    // c is ready (deps: a completed), d is NOT (deps: b completed, c pending)
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0], "c");
}
