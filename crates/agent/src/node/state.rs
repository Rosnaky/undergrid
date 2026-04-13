use std::collections::HashMap;

use raft::RaftNode;
use tokio::time::Instant;

use crate::{orchestrator::Orchestrator, system::SystemSnapshot};

pub struct NodeState {
    pub hostname: String,
    pub bind_address: String,
    pub port: u16,
    pub started_at: Instant,
    pub last_snapshot: Option<SystemSnapshot>,
    pub peer_resources: HashMap<String, SystemSnapshot>, // node_id -> latest snapshot
    pub running_tasks: Vec<String>,
    pub cluster_id: Option<String>,
    pub raft: RaftNode,
    pub orchestrator: Orchestrator,
}

impl NodeState {
    pub fn new(
        node_id: String,
        hostname: String,
        bind_address: String,
        port: u16,
        orchestrator: Orchestrator,
    ) -> Self {
        NodeState {
            hostname,
            bind_address,
            port,
            started_at: Instant::now(),
            last_snapshot: None,
            peer_resources: HashMap::new(),
            running_tasks: Vec::new(),
            cluster_id: None,
            raft: RaftNode::new(node_id.clone(), Vec::new()),
            orchestrator,
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn gather_cluster_resources(&self) -> HashMap<String, &SystemSnapshot> {
        let mut resources = HashMap::new();

        if let Some(ref snapshot) = self.last_snapshot {
            resources.insert(self.raft.node_id.clone(), snapshot);
        }

        for (node_id, snapshot) in &self.peer_resources {
            resources.insert(node_id.clone(), snapshot);
        }

        resources
    }
}
