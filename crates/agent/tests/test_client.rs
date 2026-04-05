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
    )))
}

#[tokio::test]
async fn ping_returns_node_id() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state);

    let resp = service.ping(Request::new(PingRequest {
        from_node_id: "test".to_string(),
    })).await.unwrap().into_inner();

    assert_eq!(resp.node_id, "node-1");
    assert!(resp.uptime_secs < 2);
}

#[tokio::test]
async fn register_adds_peer() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service.register(Request::new(RegisterRequest {
        node_info: Some(NodeInfo {
            node_id: "joiner-1".to_string(),
            hostname: "host-2".to_string(),
            ip_address: "127.0.0.1".to_string(),
            port: 7071,
            resources: None,
        }),
    })).await.unwrap().into_inner();

    assert!(resp.accepted);

    let s = state.read().await;
    assert!(s.raft.is_peer_by_node_id("joiner-1"));
}

#[tokio::test]
async fn register_returns_peer_list() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state.clone());

    // Add first peer
    service.register(Request::new(RegisterRequest {
        node_info: Some(NodeInfo {
            node_id: "node-2".to_string(),
            hostname: "host-2".to_string(),
            ip_address: "127.0.0.1".to_string(),
            port: 7071,
            resources: None,
        }),
    })).await.unwrap();

    // Add second peer — should get first peer in response
    let resp = service.register(Request::new(RegisterRequest {
        node_info: Some(NodeInfo {
            node_id: "node-3".to_string(),
            hostname: "host-3".to_string(),
            ip_address: "127.0.0.1".to_string(),
            port: 7072,
            resources: None,
        }),
    })).await.unwrap().into_inner();

    let peer_ids: Vec<String> = resp.peers.iter()
        .map(|p| p.node_id.clone()).collect();
    assert!(peer_ids.contains(&"node-2".to_string()));
}

#[tokio::test]
async fn register_rejects_missing_node_info() {
    let state = test_state("leader-1");
    let service = NodeAgentService::new(state);

    let result = service.register(Request::new(RegisterRequest {
        node_info: None,
    })).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn vote_grants_for_higher_term() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state);

    let resp = service.vote(Request::new(VoteRequest {
        candidate_id: "node-2".to_string(),
        term: 1,
    })).await.unwrap().into_inner();

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

    let resp = service.vote(Request::new(VoteRequest {
        candidate_id: "node-2".to_string(),
        term: 3,
    })).await.unwrap().into_inner();

    assert!(!resp.granted);
    assert_eq!(resp.term, 5);
}

#[tokio::test]
async fn append_entries_accepted() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service.append_entries(Request::new(AppendEntriesRequest {
        leader_id: "leader-1".to_string(),
        term: 1,
    })).await.unwrap().into_inner();

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

    let resp = service.append_entries(Request::new(AppendEntriesRequest {
        leader_id: "old-leader".to_string(),
        term: 3,
    })).await.unwrap().into_inner();

    assert!(!resp.success);
}

#[tokio::test]
async fn add_peer_works() {
    let state = test_state("node-1");
    let service = NodeAgentService::new(state.clone());

    let resp = service.add_peer(Request::new(AddPeerRequest {
        peer_node_info: Some(NodeInfo {
            node_id: "node-2".to_string(),
            hostname: "host-2".to_string(),
            ip_address: "127.0.0.1".to_string(),
            port: 7071,
            resources: None,
        }),
    })).await.unwrap().into_inner();

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

    let resp = service.remove_peer(Request::new(RemovePeerRequest {
        peer_node_id: "node-2".to_string(),
    })).await.unwrap().into_inner();

    assert!(resp.success);
    let s = state.read().await;
    assert!(!s.raft.is_peer_by_node_id("node-2"));
}