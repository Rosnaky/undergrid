use agent::defines::OFFLINE_TIMEOUT_MS;
use agent::orchestrator::Orchestrator;
use agent::system::{CpuInfo, DiskInfo, MemoryInfo, SystemSnapshot};
use mesh::undergrid::{Batch, TaskSpec, task_spec};
use raft::Peer;
use scheduler::NodeResources;
use scheduler::drf::DrfScheduler;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;

use agent::client::client_pool::ClientPool;
use agent::node::state::NodeState;
use agent::server::node_agent::NodeAgentService;
use mesh::undergrid::node_agent_server::NodeAgentServer;

// ── Port Allocator ───────────────────────────────────────

static PORT_COUNTER: AtomicU16 = AtomicU16::new(19000);

fn allocate_ports(count: u16) -> Vec<u16> {
    let start = PORT_COUNTER.fetch_add(count, Ordering::SeqCst);
    if start.checked_add(count).is_none() || start + count > 65000 {
        panic!("Port allocator overflow: exhausted available test ports");
    }
    (start..start + count).collect()
}

fn allocate_port() -> u16 {
    allocate_ports(1)[0]
}

// ── Offline Timeout Helpers ──────────────────────────────

async fn backdate_peer_last_seen(
    state: &Arc<RwLock<NodeState>>,
    peer_node_id: &str,
    age: Duration,
) {
    let mut s = state.write().await;
    if let Some(peer) = s.raft.peers.iter_mut().find(|p| p.node_id == peer_node_id) {
        peer.last_seen = Instant::now() - age;
    }
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

// Job tests
fn make_proto_service_task(id: &str, deps: Vec<&str>) -> TaskSpec {
    use mesh::undergrid::{PortMapping as ProtoPortMapping, RestartConfig, Service};

    TaskSpec {
        id: id.to_string(),
        image: "nginx:latest".to_string(),
        command: vec![],
        env: HashMap::new(),
        cpu_cores: 1.0,
        memory_bytes: 512,
        disk_bytes: 1000,
        gpu: false,
        depends_on: deps.into_iter().map(String::from).collect(),
        kind: Some(task_spec::Kind::Service(Service {
            health_check: "/health".to_string(),
            restart_config: Some(RestartConfig {
                policy: 0, // ALWAYS
                max_retries: 0,
                retry_delay_s: 0,
            }),
            ports: vec![ProtoPortMapping {
                container_port: 80,
                protocol: "tcp".to_string(),
            }],
        })),
    }
}

fn submit_tasks_to_orchestrator(state: &mut NodeState, job_id: &str, proto_tasks: Vec<TaskSpec>) {
    let task_specs: Vec<runtime::task::TaskSpec> = proto_tasks
        .into_iter()
        .map(|t| runtime::task::TaskSpec::try_from(t).unwrap())
        .collect();
    let task_map: HashMap<String, runtime::task::Task> = task_specs
        .into_iter()
        .map(|spec| {
            let task = runtime::task::Task::new(spec);
            (task.spec.id.clone(), task)
        })
        .collect();
    state
        .orchestrator
        .submit_job(runtime::job::JobSpec {
            id: job_id.to_string(),
            tasks: task_map,
        })
        .unwrap();
}

async fn find_leader_state(n1: &TestNode, n2: &TestNode, n3: &TestNode) -> Arc<RwLock<NodeState>> {
    if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        n1.state.clone()
    } else if matches!(get_role(&n2.state).await, raft::Role::Leader) {
        n2.state.clone()
    } else {
        n3.state.clone()
    }
}

fn gather_test_resources(s: &NodeState) -> Vec<NodeResources> {
    let mut resources = Vec::new();
    if let Some(ref snapshot) = s.last_snapshot {
        resources.push(NodeResources {
            node_id: s.raft.node_id.clone(),
            available_cpu: snapshot.cpu.cpu_cores as f64,
            available_memory_bytes: snapshot.memory.memory_available_bytes,
            available_disk_bytes: snapshot.disk.disk_available_bytes,
            available_gpu: 0,
        });
    }
    for (node_id, snapshot) in &s.peer_resources {
        resources.push(NodeResources {
            node_id: node_id.clone(),
            available_cpu: snapshot.cpu.cpu_cores as f64,
            available_memory_bytes: snapshot.memory.memory_available_bytes,
            available_disk_bytes: snapshot.disk.disk_available_bytes,
            available_gpu: 0,
        });
    }
    resources
}

async fn set_fake_resources(state: &Arc<RwLock<NodeState>>) {
    let mut s = state.write().await;
    let snapshot = SystemSnapshot {
        cpu: CpuInfo {
            cpu_cores: 4,
            cpu_usage_pct: 10.0,
            cpu_freq_mhz: 1.0,
        },
        memory: MemoryInfo {
            memory_total_bytes: 8_000_000,
            memory_available_bytes: 6_000_000,
        },
        disk: DiskInfo {
            disk_total_bytes: 100_000_000,
            disk_available_bytes: 80_000_000,
        },
        gpu: None,
        hostname: "0.0.0.0".to_string(),
    };
    s.last_snapshot = Some(snapshot.clone());

    let peer_ids: Vec<String> = s.raft.peers.iter().map(|p| p.node_id.clone()).collect();
    for peer_id in peer_ids {
        s.peer_resources.insert(peer_id, snapshot.clone());
    }
}

// ── Test Node ────────────────────────────────────────────

struct TestNode {
    state: Arc<RwLock<NodeState>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    port: u16,
    node_id: String,
}

async fn start_node(id: &str, port: u16) -> TestNode {
    let state = Arc::new(RwLock::new(NodeState::new(
        id.to_string(),
        "test-host".to_string(),
        "127.0.0.1".to_string(),
        port,
        Orchestrator::new(DrfScheduler),
    )));

    let service = NodeAgentService::new(state.clone());
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(NodeAgentServer::new(service))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            }),
    );

    sleep(Duration::from_millis(200)).await;

    TestNode {
        state,
        shutdown_tx: Some(shutdown_tx),
        port,
        node_id: id.to_string(),
    }
}

fn shutdown_node(mut node: TestNode) {
    if let Some(tx) = node.shutdown_tx.take() {
        let _ = tx.send(());
    }
}

// ── Peer Wiring ──────────────────────────────────────────

fn make_peer(id: &str, port: u16) -> raft::Peer {
    raft::Peer {
        node_id: id.to_string(),
        hostname: "test".to_string(),
        ip_address: "127.0.0.1".to_string(),
        port: port as u32,
        last_seen: Instant::now(),
        status: raft::Status::Operational,
    }
}

async fn wire_peers(nodes: &[&TestNode]) {
    for node in nodes {
        let mut s = node.state.write().await;
        for other in nodes {
            if other.node_id != node.node_id {
                s.raft.add_peer(make_peer(&other.node_id, other.port));
            }
        }
    }
}

// ── Raft Tick Runner ─────────────────────────────────────

async fn run_raft_ticks(nodes: &[&Arc<RwLock<NodeState>>], pool: &ClientPool, duration: Duration) {
    let start = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    while start.elapsed() < duration {
        interval.tick().await;
        for state in nodes {
            agent::node::runner::handle_raft_tick(state, pool).await;
        }
    }
}

// ── Assertion Helpers ────────────────────────────────────

async fn count_leaders(states: &[&Arc<RwLock<NodeState>>]) -> usize {
    let mut count = 0;
    for s in states {
        let state = s.read().await;
        if matches!(state.raft.role, raft::Role::Leader) {
            count += 1;
        }
    }
    count
}

async fn get_leader_id(states: &[&Arc<RwLock<NodeState>>]) -> Option<String> {
    for s in states {
        let state = s.read().await;
        if state.raft.leader_id.is_some() {
            return state.raft.leader_id.clone();
        }
    }
    None
}

async fn all_agree_on_leader(states: &[&Arc<RwLock<NodeState>>]) -> bool {
    let mut leader_ids: Vec<Option<String>> = vec![];
    for s in states {
        let state = s.read().await;
        leader_ids.push(state.raft.leader_id.clone());
    }
    let first = &leader_ids[0];
    first.is_some() && leader_ids.iter().all(|id| id == first)
}

async fn all_same_term(states: &[&Arc<RwLock<NodeState>>]) -> bool {
    let mut terms: Vec<u64> = vec![];
    for s in states {
        let state = s.read().await;
        terms.push(state.raft.term);
    }
    terms.windows(2).all(|w| w[0] == w[1])
}

async fn get_role(state: &Arc<RwLock<NodeState>>) -> raft::Role {
    state.read().await.raft.role.clone()
}

async fn get_term(state: &Arc<RwLock<NodeState>>) -> u64 {
    state.read().await.raft.term
}

async fn get_node_id(state: &Arc<RwLock<NodeState>>) -> String {
    state.read().await.raft.node_id.clone()
}

async fn get_peers(state: &Arc<RwLock<NodeState>>) -> Vec<Peer> {
    state.read().await.raft.peers.clone()
}

// ═══════════════════════════════════════════════════════════
// SINGLE NODE
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn single_node_elects_itself() {
    let port = allocate_port();
    let n = start_node("solo", port).await;
    let pool = ClientPool::new();

    run_raft_ticks(&[&n.state], &pool, Duration::from_millis(500)).await;

    assert!(matches!(get_role(&n.state).await, raft::Role::Leader));
    assert_eq!(get_leader_id(&[&n.state]).await, Some("solo".to_string()));

    shutdown_node(n);
}

#[tokio::test]
async fn single_node_stays_leader_over_time() {
    let port = allocate_port();
    let n = start_node("solo", port).await;
    let pool = ClientPool::new();

    run_raft_ticks(&[&n.state], &pool, Duration::from_secs(3)).await;

    assert!(matches!(get_role(&n.state).await, raft::Role::Leader));
    // Term should be 1 - elected once, no re-elections
    assert_eq!(get_term(&n.state).await, 1);

    shutdown_node(n);
}

// ═══════════════════════════════════════════════════════════
// TWO NODE CLUSTER
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn two_nodes_elect_one_leader() {
    let ports = allocate_ports(2);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2]).await;

    run_raft_ticks(&[&n1.state, &n2.state], &pool, Duration::from_secs(2)).await;

    let states = [&n1.state, &n2.state];
    assert_eq!(
        count_leaders(&states).await,
        1,
        "Exactly one leader expected"
    );
    assert!(
        all_agree_on_leader(&states).await,
        "Both nodes should agree on leader"
    );

    shutdown_node(n1);
    shutdown_node(n2);
}

#[tokio::test]
async fn two_node_leader_dies_survivor_cannot_elect() {
    let ports = allocate_ports(2);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2]).await;
    run_raft_ticks(&[&n1.state, &n2.state], &pool, Duration::from_secs(2)).await;

    // Find and kill the leader
    let (leader, survivor_state) = if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        (n1, n2.state.clone())
    } else {
        (n2, n1.state.clone())
    };

    let term_before = get_term(&survivor_state).await;
    shutdown_node(leader);
    sleep(Duration::from_millis(100)).await;

    // Survivor can't elect - quorum is 2, only 1 alive
    run_raft_ticks(&[&survivor_state], &pool, Duration::from_secs(2)).await;

    assert!(!matches!(
        get_role(&survivor_state).await,
        raft::Role::Leader
    ));
    assert!(
        get_term(&survivor_state).await > term_before,
        "Term should increase from failed elections"
    );

    // Cleanup remaining node
    if Arc::try_unwrap(survivor_state).is_ok() {
        // state dropped
    }
}

// ═══════════════════════════════════════════════════════════
// THREE NODE CLUSTER
// ═══════════════════════════════════════════════════════════

async fn start_3_node_cluster() -> (TestNode, TestNode, TestNode, ClientPool) {
    let ports = allocate_ports(3);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let n3 = start_node("C", ports[2]).await;

    wire_peers(&[&n1, &n2, &n3]).await;

    (n1, n2, n3, ClientPool::new())
}

#[tokio::test]
async fn three_nodes_elect_one_leader() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let states = [&n1.state, &n2.state, &n3.state];
    assert_eq!(count_leaders(&states).await, 1, "Exactly one leader");
    assert!(
        all_agree_on_leader(&states).await,
        "All nodes agree on leader"
    );
    assert!(all_same_term(&states).await, "All nodes at same term");

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn three_nodes_stable_over_time() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(5),
    )
    .await;

    let states = [&n1.state, &n2.state, &n3.state];
    assert_eq!(count_leaders(&states).await, 1);
    assert!(all_agree_on_leader(&states).await);
    // Should have elected once and stayed stable - low term
    let term = get_term(&n1.state).await;
    assert!(
        term <= 3,
        "Term should be low if cluster is stable, got {}",
        term
    );

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn leader_shutdown_triggers_new_election() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find the leader
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (leader_node, f1_state, f2_state) = if matches!(s1_role, raft::Role::Leader) {
        (n1, n2.state.clone(), n3.state.clone())
    } else if matches!(s2_role, raft::Role::Leader) {
        (n2, n1.state.clone(), n3.state.clone())
    } else {
        (n3, n1.state.clone(), n2.state.clone())
    };

    let old_leader_id = get_node_id(&leader_node.state).await;
    let old_term = get_term(&leader_node.state).await;

    // Kill the leader
    shutdown_node(leader_node);
    sleep(Duration::from_millis(100)).await;

    // Remove dead leader from followers
    {
        let mut s = f1_state.write().await;
        s.raft.remove_peer(&old_leader_id);
    }
    {
        let mut s = f2_state.write().await;
        s.raft.remove_peer(&old_leader_id);
    }

    // Run ticks on survivors
    run_raft_ticks(&[&f1_state, &f2_state], &pool, Duration::from_secs(2)).await;

    let states = [&f1_state, &f2_state];
    assert_eq!(
        count_leaders(&states).await,
        1,
        "Surviving nodes should elect a new leader"
    );
    assert!(
        all_agree_on_leader(&states).await,
        "Survivors should agree on leader"
    );

    let new_leader_id = get_leader_id(&states).await.unwrap();
    assert_ne!(
        new_leader_id, old_leader_id,
        "New leader should not be the dead node"
    );

    let new_term = get_term(&f1_state).await;
    assert!(
        new_term > old_term,
        "Term should increase after re-election"
    );
}

#[tokio::test]
async fn follower_shutdown_leader_survives() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find the leader and a follower
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (leader_state, follower_node, remaining_state) = if matches!(s1_role, raft::Role::Leader) {
        (n1.state.clone(), n2, n3.state.clone())
    } else if matches!(s2_role, raft::Role::Leader) {
        (n2.state.clone(), n1, n3.state.clone())
    } else {
        (n3.state.clone(), n1, n2.state.clone())
    };

    let leader_id = get_node_id(&leader_state).await;
    let leader_term = get_term(&leader_state).await;

    // Kill a follower
    let dead_id = follower_node.node_id.clone();
    shutdown_node(follower_node);
    sleep(Duration::from_millis(100)).await;

    // Remove dead follower from remaining nodes
    {
        let mut s = leader_state.write().await;
        s.raft.remove_peer(&dead_id);
    }
    {
        let mut s = remaining_state.write().await;
        s.raft.remove_peer(&dead_id);
    }

    // Run ticks - leader should remain leader
    run_raft_ticks(
        &[&leader_state, &remaining_state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    assert!(
        matches!(get_role(&leader_state).await, raft::Role::Leader),
        "Leader should still be leader"
    );
    assert_eq!(
        get_node_id(&leader_state).await,
        leader_id,
        "Same node should still be leader"
    );
    assert_eq!(
        get_term(&leader_state).await,
        leader_term,
        "Term should not change"
    );
}

#[tokio::test]
async fn dead_node_rejoins_as_follower() {
    let ports = allocate_ports(3);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2]).await;
    run_raft_ticks(&[&n1.state, &n2.state], &pool, Duration::from_secs(2)).await;

    // Kill follower B
    shutdown_node(n2);
    sleep(Duration::from_millis(200)).await;

    // Remove dead peer from leader
    {
        let mut s = n1.state.write().await;
        s.raft.remove_peer("B");
    }

    // Start fresh node on same port
    let n2_new = start_node("B-new", ports[1]).await;
    {
        let mut s = n2_new.state.write().await;
        s.raft.add_peer(make_peer("A", ports[0]));
    }
    {
        let mut s = n1.state.write().await;
        s.raft.add_peer(make_peer("B-new", ports[1]));
    }

    run_raft_ticks(&[&n1.state, &n2_new.state], &pool, Duration::from_secs(2)).await;

    let states = [&n1.state, &n2_new.state];
    assert_eq!(count_leaders(&states).await, 1, "One leader expected");
    assert!(
        all_agree_on_leader(&states).await,
        "Both should agree on leader"
    );

    shutdown_node(n1);
    shutdown_node(n2_new);
}

#[tokio::test]
async fn all_nodes_shutdown_and_restart() {
    let ports_1 = allocate_ports(3);

    // Start initial cluster
    let n1 = start_node("A", ports_1[0]).await;
    let n2 = start_node("B", ports_1[1]).await;
    let n3 = start_node("C", ports_1[2]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2, &n3]).await;
    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    assert!(
        get_leader_id(&[&n1.state, &n2.state, &n3.state])
            .await
            .is_some()
    );

    // Kill everyone
    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
    sleep(Duration::from_millis(300)).await;

    // Restart on new ports
    let ports_2 = allocate_ports(3);
    let r1 = start_node("A2", ports_2[0]).await;
    let r2 = start_node("B2", ports_2[1]).await;
    let r3 = start_node("C2", ports_2[2]).await;

    wire_peers(&[&r1, &r2, &r3]).await;
    run_raft_ticks(
        &[&r1.state, &r2.state, &r3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let states = [&r1.state, &r2.state, &r3.state];
    assert_eq!(
        count_leaders(&states).await,
        1,
        "New cluster should have one leader"
    );
    assert!(
        all_agree_on_leader(&states).await,
        "All should agree on leader"
    );
    assert!(all_same_term(&states).await, "All at same term");

    shutdown_node(r1);
    shutdown_node(r2);
    shutdown_node(r3);
}

#[tokio::test]
async fn two_of_three_nodes_die_survivor_cannot_elect() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let term_before = get_term(&n1.state).await;

    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (survivor, dead1, dead2) = if !matches!(s1_role, raft::Role::Leader) {
        (n1, n2, n3)
    } else if !matches!(s2_role, raft::Role::Leader) {
        (n2, n1, n3)
    } else {
        (n3, n1, n2)
    };

    // Kill two nodes - don't remove from peer list
    shutdown_node(dead1);
    shutdown_node(dead2);
    sleep(Duration::from_millis(300)).await;

    // Survivor can't elect - needs 2 of 3 votes
    run_raft_ticks(&[&survivor.state], &pool, Duration::from_secs(2)).await;

    let role = get_role(&survivor.state).await;
    assert!(get_peers(&survivor.state).await.len() == 2);
    assert!(
        !matches!(role, raft::Role::Leader),
        "Survivor should not be leader without quorum"
    );
    assert!(
        get_term(&survivor.state).await > term_before,
        "Term should increase from failed election attempts"
    );

    shutdown_node(survivor);
}

// ═══════════════════════════════════════════════════════════
// FIVE NODE CLUSTER
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn five_nodes_elect_one_leader() {
    let ports = allocate_ports(5);
    let nodes: Vec<TestNode> = futures::future::join_all(
        ["A", "B", "C", "D", "E"]
            .iter()
            .enumerate()
            .map(|(i, id)| start_node(id, ports[i])),
    )
    .await;

    let refs: Vec<&TestNode> = nodes.iter().collect();
    wire_peers(&refs).await;

    let state_refs: Vec<&Arc<RwLock<NodeState>>> = nodes.iter().map(|n| &n.state).collect();
    let pool = ClientPool::new();

    run_raft_ticks(&state_refs, &pool, Duration::from_secs(3)).await;

    assert_eq!(
        count_leaders(&state_refs).await,
        1,
        "Exactly one leader in 5-node cluster"
    );
    assert!(
        all_agree_on_leader(&state_refs).await,
        "All 5 nodes agree on leader"
    );
    assert!(all_same_term(&state_refs).await, "All 5 nodes at same term");

    for n in nodes {
        shutdown_node(n);
    }
}

#[tokio::test]
async fn five_nodes_tolerate_two_failures() {
    let ports = allocate_ports(5);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let n3 = start_node("C", ports[2]).await;
    let n4 = start_node("D", ports[3]).await;
    let n5 = start_node("E", ports[4]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2, &n3, &n4, &n5]).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state, &n4.state, &n5.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let states_all = [&n1.state, &n2.state, &n3.state, &n4.state, &n5.state];
    assert_eq!(count_leaders(&states_all).await, 1);

    // Kill two nodes
    let dead1_id = n4.node_id.clone();
    let dead2_id = n5.node_id.clone();
    shutdown_node(n4);
    shutdown_node(n5);
    sleep(Duration::from_millis(200)).await;

    // Remove dead peers
    for state in [&n1.state, &n2.state, &n3.state] {
        let mut s = state.write().await;
        s.raft.remove_peer(&dead1_id);
        s.raft.remove_peer(&dead2_id);
    }

    // Surviving 3 should still have a leader
    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let states_surviving = [&n1.state, &n2.state, &n3.state];
    assert_eq!(
        count_leaders(&states_surviving).await,
        1,
        "3 surviving nodes should have a leader"
    );
    assert!(all_agree_on_leader(&states_surviving).await);

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

// ═══════════════════════════════════════════════════════════
// LEADER RE-ELECTION AFTER RECOVERY
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn killed_leader_rejoins_as_follower() {
    let ports = allocate_ports(3);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let n3 = start_node("C", ports[2]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2, &n3]).await;
    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find leader
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (leader_node, survivor1, survivor2) = if matches!(s1_role, raft::Role::Leader) {
        (n1, n2, n3)
    } else if matches!(s2_role, raft::Role::Leader) {
        (n2, n1, n3)
    } else {
        (n3, n1, n2)
    };

    let old_leader_id = leader_node.node_id.clone();
    let old_leader_port = leader_node.port;

    // Kill leader
    shutdown_node(leader_node);
    sleep(Duration::from_millis(200)).await;

    // Remove from survivors
    {
        let mut s = survivor1.state.write().await;
        s.raft.remove_peer(&old_leader_id);
    }
    {
        let mut s = survivor2.state.write().await;
        s.raft.remove_peer(&old_leader_id);
    }

    // Elect new leader among survivors
    run_raft_ticks(
        &[&survivor1.state, &survivor2.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let new_leader_id = get_leader_id(&[&survivor1.state, &survivor2.state])
        .await
        .unwrap();
    assert_ne!(new_leader_id, old_leader_id);

    // Restart old leader with new identity
    let revived = start_node("A-revived", old_leader_port).await;
    {
        let mut s = revived.state.write().await;
        s.raft
            .add_peer(make_peer(&survivor1.node_id, survivor1.port));
        s.raft
            .add_peer(make_peer(&survivor2.node_id, survivor2.port));
    }
    {
        let mut s = survivor1.state.write().await;
        s.raft.add_peer(make_peer("A-revived", old_leader_port));
    }
    {
        let mut s = survivor2.state.write().await;
        s.raft.add_peer(make_peer("A-revived", old_leader_port));
    }

    run_raft_ticks(
        &[&survivor1.state, &survivor2.state, &revived.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let all_states = [&survivor1.state, &survivor2.state, &revived.state];
    assert_eq!(
        count_leaders(&all_states).await,
        1,
        "One leader after rejoin"
    );
    assert!(
        all_agree_on_leader(&all_states).await,
        "All agree after rejoin"
    );

    // Revived node should be follower
    assert!(
        matches!(get_role(&revived.state).await, raft::Role::Follower),
        "Revived old leader should be follower"
    );

    shutdown_node(survivor1);
    shutdown_node(survivor2);
    shutdown_node(revived);
}

// ═══════════════════════════════════════════════════════════
// RAPID MEMBERSHIP CHANGES
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn nodes_join_one_at_a_time() {
    let ports = allocate_ports(4);
    let pool = ClientPool::new();

    // Start with one node
    let n1 = start_node("A", ports[0]).await;
    run_raft_ticks(&[&n1.state], &pool, Duration::from_millis(500)).await;
    assert!(matches!(get_role(&n1.state).await, raft::Role::Leader));

    // Add second node
    let n2 = start_node("B", ports[1]).await;
    {
        let mut s = n1.state.write().await;
        s.raft.add_peer(make_peer("B", ports[1]));
    }
    {
        let mut s = n2.state.write().await;
        s.raft.add_peer(make_peer("A", ports[0]));
    }

    run_raft_ticks(&[&n1.state, &n2.state], &pool, Duration::from_secs(2)).await;
    let states_2 = [&n1.state, &n2.state];
    assert_eq!(count_leaders(&states_2).await, 1);

    // Add third node
    let n3 = start_node("C", ports[2]).await;
    for (state, _) in [(&n1.state, "A"), (&n2.state, "B")] {
        let mut s = state.write().await;
        s.raft.add_peer(make_peer("C", ports[2]));
    }
    {
        let mut s = n3.state.write().await;
        s.raft.add_peer(make_peer("A", ports[0]));
        s.raft.add_peer(make_peer("B", ports[1]));
    }

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let states_3 = [&n1.state, &n2.state, &n3.state];
    assert_eq!(
        count_leaders(&states_3).await,
        1,
        "One leader after 3rd node joins"
    );
    assert!(all_agree_on_leader(&states_3).await);

    // Add fourth node
    let n4 = start_node("D", ports[3]).await;
    for state in [&n1.state, &n2.state, &n3.state] {
        let mut s = state.write().await;
        s.raft.add_peer(make_peer("D", ports[3]));
    }
    {
        let mut s = n4.state.write().await;
        s.raft.add_peer(make_peer("A", ports[0]));
        s.raft.add_peer(make_peer("B", ports[1]));
        s.raft.add_peer(make_peer("C", ports[2]));
    }

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state, &n4.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let states_4 = [&n1.state, &n2.state, &n3.state, &n4.state];
    assert_eq!(
        count_leaders(&states_4).await,
        1,
        "One leader after 4th node joins"
    );
    assert!(all_agree_on_leader(&states_4).await);

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
    shutdown_node(n4);
}

// ═══════════════════════════════════════════════════════════
// OFFLINE DETECTION & AUTO-REMOVAL
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn leader_auto_removes_offline_peer() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find the leader
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (leader_state, follower_node, remaining) = if matches!(s1_role, raft::Role::Leader) {
        (n1.state.clone(), n2, n3)
    } else if matches!(s2_role, raft::Role::Leader) {
        (n2.state.clone(), n1, n3)
    } else {
        (n3.state.clone(), n1, n2)
    };

    let dead_id = follower_node.node_id.clone();

    // Kill a follower
    shutdown_node(follower_node);
    sleep(Duration::from_millis(100)).await;

    // Backdate the dead peer so offline timeout fires on next tick
    backdate_peer_last_seen(
        &leader_state,
        &dead_id,
        Duration::from_millis(OFFLINE_TIMEOUT_MS + 1000),
    )
    .await;

    // One tick is enough to trigger removal
    run_raft_ticks(
        &[&leader_state, &remaining.state],
        &pool,
        Duration::from_millis(500),
    )
    .await;

    // Leader should have removed the dead peer
    let leader_peers = get_peers(&leader_state).await;
    assert!(
        !leader_peers.iter().any(|p| p.node_id == dead_id),
        "Dead peer should be auto-removed from leader"
    );
    assert_eq!(leader_peers.len(), 1, "Leader should have 1 remaining peer");

    // Leader should still be leader
    assert!(matches!(get_role(&leader_state).await, raft::Role::Leader));

    shutdown_node(remaining);
}

#[tokio::test]
async fn leader_broadcasts_removal_to_remaining_peers() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (leader_state, follower_node, remaining_state) = if matches!(s1_role, raft::Role::Leader) {
        (n1.state.clone(), n2, n3.state.clone())
    } else if matches!(s2_role, raft::Role::Leader) {
        (n2.state.clone(), n1, n3.state.clone())
    } else {
        (n3.state.clone(), n1, n2.state.clone())
    };

    let dead_id = follower_node.node_id.clone();
    shutdown_node(follower_node);
    sleep(Duration::from_millis(100)).await;

    // Backdate on leader so offline detection fires
    backdate_peer_last_seen(
        &leader_state,
        &dead_id,
        Duration::from_millis(OFFLINE_TIMEOUT_MS + 1000),
    )
    .await;

    // Run ticks — leader detects offline, sends RemovePeer to remaining node
    run_raft_ticks(
        &[&leader_state, &remaining_state],
        &pool,
        Duration::from_secs(1),
    )
    .await;

    // Remaining node should also have the dead peer removed (via RemovePeer RPC)
    let remaining_peers = get_peers(&remaining_state).await;
    assert!(
        !remaining_peers.iter().any(|p| p.node_id == dead_id),
        "Remaining node should have dead peer removed via broadcast"
    );
}

#[tokio::test]
async fn offline_peer_rejoins_after_auto_removal() {
    let ports = allocate_ports(3);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let n3 = start_node("C", ports[2]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2, &n3]).await;
    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find leader
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (leader, survivor, victim) = if matches!(s1_role, raft::Role::Leader) {
        (n1, n2, n3)
    } else if matches!(s2_role, raft::Role::Leader) {
        (n2, n1, n3)
    } else {
        (n3, n1, n2)
    };

    let victim_id = victim.node_id.clone();
    let victim_port = victim.port;
    shutdown_node(victim);
    sleep(Duration::from_millis(100)).await;

    // Backdate and let leader auto-remove
    backdate_peer_last_seen(
        &leader.state,
        &victim_id,
        Duration::from_millis(OFFLINE_TIMEOUT_MS + 1000),
    )
    .await;
    run_raft_ticks(
        &[&leader.state, &survivor.state],
        &pool,
        Duration::from_secs(1),
    )
    .await;

    // Verify removal happened
    assert!(
        !get_peers(&leader.state)
            .await
            .iter()
            .any(|p| p.node_id == victim_id)
    );

    // Restart the dead node with a new identity
    let revived = start_node("C-revived", victim_port).await;
    {
        let mut s = revived.state.write().await;
        s.raft.add_peer(make_peer(&leader.node_id, leader.port));
        s.raft.add_peer(make_peer(&survivor.node_id, survivor.port));
    }
    {
        let mut s = leader.state.write().await;
        s.raft.add_peer(make_peer("C-revived", victim_port));
    }
    {
        let mut s = survivor.state.write().await;
        s.raft.add_peer(make_peer("C-revived", victim_port));
    }

    run_raft_ticks(
        &[&leader.state, &survivor.state, &revived.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let all = [&leader.state, &survivor.state, &revived.state];
    assert_eq!(count_leaders(&all).await, 1, "One leader after rejoin");
    assert!(all_agree_on_leader(&all).await);
    assert!(
        matches!(get_role(&revived.state).await, raft::Role::Follower),
        "Revived node should be follower"
    );

    shutdown_node(leader);
    shutdown_node(survivor);
    shutdown_node(revived);
}

#[tokio::test]
async fn leader_handles_multiple_peers_going_offline() {
    let ports = allocate_ports(5);
    let n1 = start_node("A", ports[0]).await;
    let n2 = start_node("B", ports[1]).await;
    let n3 = start_node("C", ports[2]).await;
    let n4 = start_node("D", ports[3]).await;
    let n5 = start_node("E", ports[4]).await;
    let pool = ClientPool::new();

    wire_peers(&[&n1, &n2, &n3, &n4, &n5]).await;
    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state, &n4.state, &n5.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Force n1 to be leader for determinism — find actual leader
    let states_all = [&n1.state, &n2.state, &n3.state, &n4.state, &n5.state];
    assert_eq!(count_leaders(&states_all).await, 1);

    // Kill D and E
    let dead1_id = n4.node_id.clone();
    let dead2_id = n5.node_id.clone();
    shutdown_node(n4);
    shutdown_node(n5);
    sleep(Duration::from_millis(100)).await;

    // Find the leader among survivors and backdate dead peers
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let leader_state = if matches!(s1_role, raft::Role::Leader) {
        &n1.state
    } else if matches!(s2_role, raft::Role::Leader) {
        &n2.state
    } else {
        &n3.state
    };

    backdate_peer_last_seen(
        leader_state,
        &dead1_id,
        Duration::from_millis(OFFLINE_TIMEOUT_MS + 1000),
    )
    .await;
    backdate_peer_last_seen(
        leader_state,
        &dead2_id,
        Duration::from_millis(OFFLINE_TIMEOUT_MS + 1000),
    )
    .await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Leader should have removed both dead peers
    let leader_peers = get_peers(leader_state).await;
    assert!(
        !leader_peers
            .iter()
            .any(|p| p.node_id == dead1_id || p.node_id == dead2_id),
        "Both dead peers should be auto-removed"
    );

    // Cluster of 3 should still function
    let states_surviving = [&n1.state, &n2.state, &n3.state];
    assert_eq!(count_leaders(&states_surviving).await, 1);
    assert!(all_agree_on_leader(&states_surviving).await);

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn only_leader_triggers_offline_removal() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find a follower
    let s1_role = get_role(&n1.state).await;
    let s2_role = get_role(&n2.state).await;

    let (follower_state, other1, other2) = if !matches!(s1_role, raft::Role::Leader) {
        (&n1.state, n2, n3)
    } else if !matches!(s2_role, raft::Role::Leader) {
        (&n2.state, n1, n3)
    } else {
        (&n3.state, n1, n2)
    };

    // Backdate a peer on the follower — should have no effect
    let peer_id = {
        let s = follower_state.read().await;
        s.raft.peers[0].node_id.clone()
    };
    backdate_peer_last_seen(
        follower_state,
        &peer_id,
        Duration::from_millis(OFFLINE_TIMEOUT_MS + 1000),
    )
    .await;

    let peers_before = get_peers(follower_state).await.len();
    run_raft_ticks(&[follower_state], &pool, Duration::from_millis(500)).await;
    let peers_after = get_peers(follower_state).await.len();

    assert_eq!(
        peers_before, peers_after,
        "Follower should NOT auto-remove peers — only leader does that"
    );

    shutdown_node(other1);
    shutdown_node(other2);
}

// ═══════════════════════════════════════════════════════════
// ORCHESTRATOR TICK
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn orchestrator_tick_dispatches_ready_tasks() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    // Set fake resources on all nodes
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    // Elect a leader
    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find leader
    let leader_state = if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        n1.state.clone()
    } else if matches!(get_role(&n2.state).await, raft::Role::Leader) {
        n2.state.clone()
    } else {
        n3.state.clone()
    };

    // Submit a job directly to orchestrator
    {
        let mut s = leader_state.write().await;
        let tasks = vec![
            make_proto_task("a", vec![]),
            make_proto_task("b", vec!["a"]),
        ];
        let task_specs: Vec<runtime::task::TaskSpec> = tasks
            .into_iter()
            .map(|t| runtime::task::TaskSpec::try_from(t).unwrap())
            .collect();
        let task_map: HashMap<String, runtime::task::Task> = task_specs
            .into_iter()
            .map(|spec| {
                let task = runtime::task::Task::new(spec);
                (task.spec.id.clone(), task)
            })
            .collect();
        s.orchestrator
            .submit_job(runtime::job::JobSpec {
                id: "job-1".to_string(),
                tasks: task_map,
            })
            .unwrap();
    }

    // Run orchestrator tick — should schedule task a
    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;

    // Task a should now be Running
    let s = leader_state.read().await;
    let job = &s.orchestrator.jobs["job-1"];
    assert!(
        matches!(
            job.spec.tasks["a"].state,
            runtime::task::TaskState::Running { .. }
        ),
        "Task a should be Running after orchestrator tick"
    );
    assert!(
        matches!(job.spec.tasks["b"].state, runtime::task::TaskState::Pending),
        "Task b should still be Pending"
    );

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn orchestrator_tick_skipped_on_follower() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;

    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    // Find a follower
    let follower_state = if !matches!(get_role(&n1.state).await, raft::Role::Leader) {
        &n1.state.clone()
    } else if !matches!(get_role(&n2.state).await, raft::Role::Leader) {
        &n2.state.clone()
    } else {
        &n3.state.clone()
    };

    // Submit job directly to follower's orchestrator
    {
        let mut s = follower_state.write().await;
        let task_map: HashMap<String, runtime::task::Task> = vec![(
            "a".to_string(),
            runtime::task::Task::new(
                runtime::task::TaskSpec::try_from(make_proto_task("a", vec![])).unwrap(),
            ),
        )]
        .into_iter()
        .collect();
        s.orchestrator
            .submit_job(runtime::job::JobSpec {
                id: "job-1".to_string(),
                tasks: task_map,
            })
            .unwrap();
    }

    // Orchestrator tick on follower should be a no-op
    agent::node::runner::handle_orchestrator_tick(follower_state, &pool).await;

    let s = follower_state.read().await;
    let job = &s.orchestrator.jobs["job-1"];
    assert!(
        matches!(job.spec.tasks["a"].state, runtime::task::TaskState::Pending),
        "Follower should not schedule tasks"
    );

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

// ═══════════════════════════════════════════════════════════
// BATCH JOB TESTS
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn batch_job_schedules_independent_tasks_in_parallel() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = find_leader_state(&n1, &n2, &n3).await;

    let nodes = {
        let s = leader_state.read().await;
        gather_test_resources(&s)
    };

    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(
            &mut s,
            "batch-parallel",
            vec![
                make_proto_task("t1", vec![]),
                make_proto_task("t2", vec![]),
                make_proto_task("t3", vec![]),
            ],
        );

        let dispatches = s
            .orchestrator
            .schedule_ready_tasks("batch-parallel", &nodes);
        assert_eq!(
            dispatches.len(),
            3,
            "All 3 independent tasks should be scheduled"
        );

        let job = &s.orchestrator.jobs["batch-parallel"];
        for id in ["t1", "t2", "t3"] {
            assert!(
                matches!(
                    job.spec.tasks[id].state,
                    runtime::task::TaskState::Running { .. }
                ),
                "Task {} should be Running",
                id
            );
        }
        assert!(matches!(job.state, runtime::job::JobState::Running { .. }));
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn batch_job_respects_dag_ordering() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        n1.state.clone()
    } else if matches!(get_role(&n2.state).await, raft::Role::Leader) {
        n2.state.clone()
    } else {
        n3.state.clone()
    };

    // a -> b -> c (linear chain)
    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(
            &mut s,
            "batch-chain",
            vec![
                make_proto_task("a", vec![]),
                make_proto_task("b", vec!["a"]),
                make_proto_task("c", vec!["b"]),
            ],
        );
    }

    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;

    {
        let s = leader_state.read().await;
        let job = &s.orchestrator.jobs["batch-chain"];
        assert!(matches!(
            job.spec.tasks["a"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            job.spec.tasks["b"].state,
            runtime::task::TaskState::Pending
        ));
        assert!(matches!(
            job.spec.tasks["c"].state,
            runtime::task::TaskState::Pending
        ));
    }

    // Simulate task a completing
    {
        let mut s = leader_state.write().await;
        s.orchestrator
            .handle_task_result(
                &"batch-chain".to_string(),
                &"a".to_string(),
                true,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: vec![],
                    exit_code: 0,
                },
            )
            .unwrap();
    }

    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;

    {
        let s = leader_state.read().await;
        let job = &s.orchestrator.jobs["batch-chain"];
        assert!(matches!(
            job.spec.tasks["a"].state,
            runtime::task::TaskState::Completed { .. }
        ));
        assert!(matches!(
            job.spec.tasks["b"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            job.spec.tasks["c"].state,
            runtime::task::TaskState::Pending
        ));
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn batch_job_diamond_dag() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = find_leader_state(&n1, &n2, &n3).await;

    let nodes = {
        let s = leader_state.read().await;
        gather_test_resources(&s)
    };

    //   a
    //  / \
    // b   c
    //  \ /
    //   d
    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(
            &mut s,
            "diamond",
            vec![
                make_proto_task("a", vec![]),
                make_proto_task("b", vec!["a"]),
                make_proto_task("c", vec!["a"]),
                make_proto_task("d", vec!["b", "c"]),
            ],
        );
    }

    // Wave 1: only a
    {
        let mut s = leader_state.write().await;
        let dispatches = s.orchestrator.schedule_ready_tasks("diamond", &nodes);
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].0.id, "a");

        let job = &s.orchestrator.jobs["diamond"];
        assert!(matches!(
            job.spec.tasks["a"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            job.spec.tasks["b"].state,
            runtime::task::TaskState::Pending
        ));
        assert!(matches!(
            job.spec.tasks["c"].state,
            runtime::task::TaskState::Pending
        ));
        assert!(matches!(
            job.spec.tasks["d"].state,
            runtime::task::TaskState::Pending
        ));
    }

    // Complete a -> wave 2: b and c
    {
        let mut s = leader_state.write().await;
        s.orchestrator
            .handle_task_result(
                &"diamond".to_string(),
                &"a".to_string(),
                true,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: vec![],
                    exit_code: 0,
                },
            )
            .unwrap();

        let dispatches = s.orchestrator.schedule_ready_tasks("diamond", &nodes);
        assert_eq!(dispatches.len(), 2, "b and c should both be dispatched");

        let job = &s.orchestrator.jobs["diamond"];
        assert!(matches!(
            job.spec.tasks["b"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            job.spec.tasks["c"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            job.spec.tasks["d"].state,
            runtime::task::TaskState::Pending
        ));
    }

    // Complete b only -> d still blocked on c
    {
        let mut s = leader_state.write().await;
        s.orchestrator
            .handle_task_result(
                &"diamond".to_string(),
                &"b".to_string(),
                true,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: vec![],
                    exit_code: 0,
                },
            )
            .unwrap();

        let dispatches = s.orchestrator.schedule_ready_tasks("diamond", &nodes);
        assert_eq!(dispatches.len(), 0, "d should not be ready yet");

        let job = &s.orchestrator.jobs["diamond"];
        assert!(
            matches!(job.spec.tasks["d"].state, runtime::task::TaskState::Pending),
            "d should still be Pending — c hasn't completed"
        );
    }

    // Complete c -> d becomes ready
    {
        let mut s = leader_state.write().await;
        s.orchestrator
            .handle_task_result(
                &"diamond".to_string(),
                &"c".to_string(),
                true,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: vec![],
                    exit_code: 0,
                },
            )
            .unwrap();

        let dispatches = s.orchestrator.schedule_ready_tasks("diamond", &nodes);
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].0.id, "d");

        let job = &s.orchestrator.jobs["diamond"];
        assert!(matches!(
            job.spec.tasks["d"].state,
            runtime::task::TaskState::Running { .. }
        ));
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn batch_task_failure_does_not_block_independent_tasks() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = find_leader_state(&n1, &n2, &n3).await;

    let nodes = {
        let s = leader_state.read().await;
        gather_test_resources(&s)
    };

    // a and b are independent roots, c depends on a, d depends on b
    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(
            &mut s,
            "partial-fail",
            vec![
                make_proto_task("a", vec![]),
                make_proto_task("b", vec![]),
                make_proto_task("c", vec!["a"]),
                make_proto_task("d", vec!["b"]),
            ],
        );

        let dispatches = s.orchestrator.schedule_ready_tasks("partial-fail", &nodes);
        assert_eq!(dispatches.len(), 2, "a and b should both be scheduled");
    }

    // Fail task a, complete task b
    {
        let mut s = leader_state.write().await;
        s.orchestrator
            .handle_task_result(
                &"partial-fail".to_string(),
                &"a".to_string(),
                false,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: b"crash".to_vec(),
                    exit_code: 1,
                },
            )
            .unwrap();
        s.orchestrator
            .handle_task_result(
                &"partial-fail".to_string(),
                &"b".to_string(),
                true,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: vec![],
                    exit_code: 0,
                },
            )
            .unwrap();

        let dispatches = s.orchestrator.schedule_ready_tasks("partial-fail", &nodes);
        assert_eq!(dispatches.len(), 1, "Only d should be dispatched");
        assert_eq!(dispatches[0].0.id, "d");

        let job = &s.orchestrator.jobs["partial-fail"];
        assert!(matches!(
            job.spec.tasks["a"].state,
            runtime::task::TaskState::Failed { .. }
        ));
        assert!(
            matches!(job.spec.tasks["c"].state, runtime::task::TaskState::Pending),
            "c should stay Pending — its dependency a failed"
        );
        assert!(
            matches!(
                job.spec.tasks["d"].state,
                runtime::task::TaskState::Running { .. }
            ),
            "d should be Running — its dependency b succeeded"
        );
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

// ═══════════════════════════════════════════════════════════
// SERVICE JOB TESTS
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn service_task_parses_and_schedules() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        n1.state.clone()
    } else if matches!(get_role(&n2.state).await, raft::Role::Leader) {
        n2.state.clone()
    } else {
        n3.state.clone()
    };

    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(
            &mut s,
            "svc-job",
            vec![make_proto_service_task("web", vec![])],
        );
    }

    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;

    {
        let s = leader_state.read().await;
        let job = &s.orchestrator.jobs["svc-job"];
        assert!(
            matches!(
                job.spec.tasks["web"].state,
                runtime::task::TaskState::Running { .. }
            ),
            "Service task should be scheduled"
        );
        assert!(
            matches!(
                job.spec.tasks["web"].spec.kind,
                runtime::task::TaskKind::Service { .. }
            ),
            "Task kind should be Service"
        );
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn service_task_with_restart_policy_retries() {
    use mesh::undergrid::{RestartConfig, Service};

    let proto_task = TaskSpec {
        id: "worker".to_string(),
        image: "worker:latest".to_string(),
        command: vec![],
        env: HashMap::new(),
        cpu_cores: 2.0,
        memory_bytes: 1024,
        disk_bytes: 0,
        gpu: false,
        depends_on: vec![],
        kind: Some(task_spec::Kind::Service(Service {
            health_check: String::new(),
            restart_config: Some(RestartConfig {
                policy: 1, // RETRIES
                max_retries: 3,
                retry_delay_s: 5,
            }),
            ports: vec![],
        })),
    };

    let spec = runtime::task::TaskSpec::try_from(proto_task).unwrap();
    match &spec.kind {
        runtime::task::TaskKind::Service {
            restart_policy,
            health_check,
            ..
        } => {
            match restart_policy {
                runtime::task::RestartPolicy::Retries {
                    max_retries,
                    retry_delay_s,
                } => {
                    assert_eq!(*max_retries, 3);
                    assert_eq!(*retry_delay_s, 5);
                }
                other => panic!("Expected Retries, got {:?}", other),
            }
            assert!(
                health_check.is_none(),
                "Empty health_check should map to None"
            );
        }
        other => panic!("Expected Service, got {:?}", other),
    }
}

#[tokio::test]
async fn mixed_batch_and_service_job() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        n1.state.clone()
    } else if matches!(get_role(&n2.state).await, raft::Role::Leader) {
        n2.state.clone()
    } else {
        n3.state.clone()
    };

    // Batch task "migrate" runs first, then service "api" depends on it
    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(
            &mut s,
            "mixed-job",
            vec![
                make_proto_task("migrate", vec![]),
                make_proto_service_task("api", vec!["migrate"]),
            ],
        );
    }

    // Wave 1: only migrate
    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;
    {
        let s = leader_state.read().await;
        let job = &s.orchestrator.jobs["mixed-job"];
        assert!(matches!(
            job.spec.tasks["migrate"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            job.spec.tasks["api"].state,
            runtime::task::TaskState::Pending
        ));
    }

    // Complete migrate -> api becomes ready
    {
        let mut s = leader_state.write().await;
        s.orchestrator
            .handle_task_result(
                &"mixed-job".to_string(),
                &"migrate".to_string(),
                true,
                runtime::task::TaskOutput {
                    stdout: vec![],
                    stderr: vec![],
                    exit_code: 0,
                },
            )
            .unwrap();
    }
    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;
    {
        let s = leader_state.read().await;
        let job = &s.orchestrator.jobs["mixed-job"];
        assert!(
            matches!(
                job.spec.tasks["api"].state,
                runtime::task::TaskState::Running { .. }
            ),
            "Service task should run after batch dependency completes"
        );
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}

#[tokio::test]
async fn multiple_jobs_scheduled_independently() {
    let (n1, n2, n3, pool) = start_3_node_cluster().await;
    set_fake_resources(&n1.state).await;
    set_fake_resources(&n2.state).await;
    set_fake_resources(&n3.state).await;

    run_raft_ticks(
        &[&n1.state, &n2.state, &n3.state],
        &pool,
        Duration::from_secs(2),
    )
    .await;

    let leader_state = if matches!(get_role(&n1.state).await, raft::Role::Leader) {
        n1.state.clone()
    } else if matches!(get_role(&n2.state).await, raft::Role::Leader) {
        n2.state.clone()
    } else {
        n3.state.clone()
    };

    {
        let mut s = leader_state.write().await;
        submit_tasks_to_orchestrator(&mut s, "job-a", vec![make_proto_task("t1", vec![])]);
        submit_tasks_to_orchestrator(
            &mut s,
            "job-b",
            vec![make_proto_service_task("svc1", vec![])],
        );
    }

    agent::node::runner::handle_orchestrator_tick(&leader_state, &pool).await;

    {
        let s = leader_state.read().await;
        assert!(matches!(
            s.orchestrator.jobs["job-a"].spec.tasks["t1"].state,
            runtime::task::TaskState::Running { .. }
        ));
        assert!(matches!(
            s.orchestrator.jobs["job-b"].spec.tasks["svc1"].state,
            runtime::task::TaskState::Running { .. }
        ));
    }

    shutdown_node(n1);
    shutdown_node(n2);
    shutdown_node(n3);
}
