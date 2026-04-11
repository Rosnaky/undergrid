use scheduler::drf::DrfScheduler;
use scheduler::{NodeResources, Scheduler};
use runtime::task::*;
use std::collections::HashMap;
use std::time::Duration;

fn make_spec(id: &str, cpu: f64, mem: u64, disk: u64, gpu: bool, deps: Vec<&str>) -> TaskSpec {
    TaskSpec {
        id: id.to_string(),
        image: "test:latest".to_string(),
        command: vec![],
        env: HashMap::new(),
        resources: ResourceRequirements {
            cpu_cores: cpu,
            memory_bytes: mem,
            disk_bytes: disk,
            gpu,
        },
        depends_on: deps.into_iter().map(String::from).collect(),
        kind: TaskKind::Batch {
            timeout: Duration::from_secs(60),
        },
    }
}

fn make_node(id: &str, cpu: f64, mem: u64, disk: u64, gpu: bool) -> NodeResources {
    NodeResources {
        node_id: id.to_string(),
        available_cpu: cpu,
        available_memory_bytes: mem,
        available_disk_bytes: disk,
        available_gpu: gpu as u32,
    }
}

#[test]
fn single_task_single_node() {
    let scheduler = DrfScheduler;
    let spec = make_spec("t1", 1.0, 512, 1000, false, vec![]);
    let nodes = vec![make_node("n1", 4.0, 2048, 10000, false)];

    let (assignments, errors) = scheduler.schedule(&[&spec], &nodes).unwrap();

    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].task_id, "t1");
    assert_eq!(assignments[0].node_id, "n1");
    assert!(errors.is_empty());
}

#[test]
fn task_too_large_for_any_node() {
    let scheduler = DrfScheduler;
    let spec = make_spec("t1", 16.0, 512, 1000, false, vec![]);
    let nodes = vec![
        make_node("n1", 4.0, 2048, 10000, false),
        make_node("n2", 8.0, 4096, 20000, false),
    ];

    let (assignments, errors) = scheduler.schedule(&[&spec], &nodes).unwrap();

    assert!(assignments.is_empty());
    assert_eq!(errors.len(), 1);
}

#[test]
fn best_fit_picks_tightest_node() {
    let scheduler = DrfScheduler;
    // Task needs 2 CPU — should go to the 2-core node, not the 8-core one
    let spec = make_spec("t1", 2.0, 512, 1000, false, vec![]);
    let nodes = vec![
        make_node("big", 8.0, 8192, 80000, false),
        make_node("small", 2.0, 2048, 10000, false),
    ];

    let (assignments, _) = scheduler.schedule(&[&spec], &nodes).unwrap();

    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].node_id, "small");
}

#[test]
fn two_tasks_spread_across_nodes() {
    let scheduler = DrfScheduler;
    // Two tasks each needing 2 CPU, two nodes with 2 CPU each
    // After first assignment deducts resources, second must go to other node
    let t1 = make_spec("t1", 2.0, 512, 1000, false, vec![]);
    let t2 = make_spec("t2", 2.0, 512, 1000, false, vec![]);
    let nodes = vec![
        make_node("n1", 2.0, 2048, 10000, false),
        make_node("n2", 2.0, 2048, 10000, false),
    ];

    let (assignments, errors) = scheduler.schedule(&[&t1, &t2], &nodes).unwrap();

    assert_eq!(assignments.len(), 2);
    assert!(errors.is_empty());
    // Should be on different nodes
    assert_ne!(assignments[0].node_id, assignments[1].node_id);
}

#[test]
fn drf_schedules_smallest_dominant_share_first() {
    let scheduler = DrfScheduler;
    // t_mem is memory-dominant (needs 4096 of 8192 = 50% memory, but only 1/8 CPU)
    // t_cpu is cpu-dominant (needs 4 of 8 = 50% CPU, but little memory)
    // Both have same dominant share (50%), but the one with smaller share goes first
    // Make them unequal:
    let t_small = make_spec("t_small", 1.0, 256, 500, false, vec![]);  // small dominant share
    let t_large = make_spec("t_large", 4.0, 4096, 5000, false, vec![]); // large dominant share
    let nodes = vec![make_node("n1", 8.0, 8192, 80000, false)];

    let (assignments, _) = scheduler.schedule(&[&t_large, &t_small], &nodes).unwrap();

    // t_small has smaller dominant share, should be scheduled first
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].task_id, "t_small");
    assert_eq!(assignments[1].task_id, "t_large");
}

#[test]
fn gpu_task_only_on_gpu_node() {
    let scheduler = DrfScheduler;
    let spec = make_spec("t1", 1.0, 512, 1000, true, vec![]);
    let nodes = vec![
        make_node("cpu_only", 8.0, 8192, 80000, false),
        make_node("gpu_node", 4.0, 4096, 40000, true),
    ];

    let (assignments, _) = scheduler.schedule(&[&spec], &nodes).unwrap();

    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].node_id, "gpu_node");
}

#[test]
fn no_nodes_available() {
    let scheduler = DrfScheduler;
    let spec = make_spec("t1", 1.0, 512, 1000, false, vec![]);
    let nodes: Vec<NodeResources> = vec![];

    let (assignments, errors) = scheduler.schedule(&[&spec], &nodes).unwrap();

    assert!(assignments.is_empty());
    assert_eq!(errors.len(), 1);
}

#[test]
fn memory_constrained_placement() {
    let scheduler = DrfScheduler;
    // Task needs 3GB, only one node has enough
    let spec = make_spec("t1", 1.0, 3_000_000_000, 1000, false, vec![]);
    let nodes = vec![
        make_node("small", 4.0, 2_000_000_000, 10000, false),
        make_node("big", 4.0, 4_000_000_000, 10000, false),
    ];

    let (assignments, _) = scheduler.schedule(&[&spec], &nodes).unwrap();

    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].node_id, "big");
}

#[test]
fn three_tasks_two_nodes_one_waits() {
    let scheduler = DrfScheduler;
    // Each task needs 2 CPU, each node has 2 CPU — third task can't fit
    let t1 = make_spec("t1", 2.0, 512, 1000, false, vec![]);
    let t2 = make_spec("t2", 2.0, 512, 1000, false, vec![]);
    let t3 = make_spec("t3", 2.0, 512, 1000, false, vec![]);
    let nodes = vec![
        make_node("n1", 2.0, 2048, 10000, false),
        make_node("n2", 2.0, 2048, 10000, false),
    ];

    let (assignments, errors) = scheduler.schedule(&[&t1, &t2, &t3], &nodes).unwrap();

    assert_eq!(assignments.len(), 2);
    assert_eq!(errors.len(), 1);
}
