use raft::RaftNode;
use tokio::time::Instant;

use crate::system::system::SystemSnapshot;

// TODO: Add resources for all peers
pub struct NodeState {
    pub hostname: String,
    pub bind_address: String,
    pub port: u16,
    pub started_at: Instant,
    pub last_snapshot: Option<SystemSnapshot>,
    pub running_tasks: Vec<String>,
    pub cluster_id: Option<String>,
    pub raft: RaftNode,
}

impl NodeState {
    pub fn new(node_id: String, hostname: String, bind_address: String, port: u16) -> Self {
        NodeState {
            hostname,
            bind_address,
            port,
            started_at: Instant::now(),
            last_snapshot: None,
            running_tasks: Vec::new(),
            cluster_id: None,
            raft: RaftNode::new(node_id.clone(), Vec::new()),
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}
