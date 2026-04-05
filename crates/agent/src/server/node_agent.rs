use std::{sync::Arc, time::Instant};

use mesh::undergrid::{AddPeerRequest, AddPeerResponse, AppendEntriesRequest, AppendEntriesResponse, HeartbeatRequest, HeartbeatResponse, NodeInfo, PingRequest, PingResponse, RegisterRequest, RegisterResponse, RemovePeerRequest, RemovePeerResponse, ResourceSnapshot, VoteRequest, VoteResponse, node_agent_server::NodeAgent};
use raft::RaftMessage;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::{client::{client::add_peer, client_pool::ClientPool}, node::state::NodeState};


pub struct NodeAgentService {
    state: Arc<RwLock<NodeState>>,
    client_pool: ClientPool,
}

impl NodeAgentService {
    pub fn new(state: Arc<RwLock<NodeState>>) -> Self {
        Self {
            state,
            client_pool: ClientPool::new(),
        }
    }
}

#[tonic::async_trait]
impl NodeAgent for NodeAgentService {
    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.read().await;

        tracing::info!(from = %req.from_node_id, "Received ping");

        Ok(Response::new(PingResponse {
            node_id: state.raft.node_id.clone(),
            uptime_secs: state.uptime_secs(),
        }))
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let node_info = req.node_info
            .ok_or_else(|| Status::invalid_argument("Missing node_info"))?;

        tracing::info!(
            peer_node_id = %node_info.node_id, 
            hostname = %node_info.hostname, 
            "Registering node"
        );
        
        let state = self.state.read().await;
        let existing_peers = state.raft.peers.clone();
        let peer_node_info = NodeInfo {
            node_id: node_info.node_id.clone(),
            hostname: node_info.hostname.clone(),
            ip_address: node_info.ip_address.clone(),
            port: node_info.port as u32,
            resources: match state.last_snapshot.clone() {
                Some(s) => Some(ResourceSnapshot {
                    cpu_cores: s.cpu.cpu_cores as u64,
                    cpu_usage_pct: s.cpu.cpu_usage_pct,
                    memory_total_bytes: s.memory.memory_total_bytes,
                    memory_available_bytes: s.memory.memory_available_bytes,
                    disk_total_bytes: s.disk.disk_total_bytes,
                    disk_available_bytes: s.disk.disk_available_bytes
                }),
                _ => None
            },
        };
        drop(state);

        // Scope in closure to avoid deadlock of mutex
        {
            let mut state = self.state.write().await;
            state.raft.add_peer(raft::Peer {
                node_id: node_info.node_id.clone(),
                hostname: node_info.hostname.clone(),
                ip_address: node_info.ip_address.clone(),
                port: node_info.port,
                last_seen: Instant::now(),
                status: raft::Status::Operational,
            });
        }

        let peer_list = {
            let state = self.state.read().await;
            let mut peers: Vec<NodeInfo> = state.raft.peers.
                iter()
                .map(|p| NodeInfo {
                    node_id: p.node_id.clone(),
                    hostname: p.hostname.clone(),
                    ip_address: p.ip_address.clone(),
                    port: p.port as u32,
                    resources: None,
                })
                .collect();

            peers.push(peer_node_info.clone());
            
            peers
        };


        let state = self.state.read().await;
        let pool_clone = self.client_pool.clone();

        drop(state);
        tokio::spawn(async move {
            for peer in existing_peers {
                let _ = add_peer(&pool_clone, &peer.addr(), peer_node_info.clone()).await;
            }
        });

        // TODO: This accepts everyone for now. Change to only the leader later
        let state = self.state.read().await;

        Ok(Response::new(RegisterResponse {
            accepted: true,
            cluster_id: state.cluster_id.clone().unwrap_or_default(),
            leader_id: state.raft.node_id.clone(),
            peers: peer_list,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            from = %req.node_id,
            tasks = req.running_tasks.len(),
            "Received heartbeat"
        );

        let mut s = self.state.write().await;
        s.raft.handle_heartbeat_response(req.node_id);
        drop(s);

        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
        }))
    }

    async fn vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            from = %req.candidate_id.clone(),
            term = req.term,
            "Received request to vote"
        );

        let mut state = self.state.write().await;
        let resp: RaftMessage = state.raft.handle_vote_request(
            req.candidate_id, req.term
        );

        drop(state);

        match resp {
            RaftMessage::VoteResponse { term, granted, .. } => {
                Ok(Response::new(VoteResponse {
                    term,
                    granted,
                }))
            }
            _ => Err(Status::internal("Unexpected raft message type")),
        }
        
        
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            from = %req.leader_id.clone(),
            term = req.term,
            "Received request to append entries from leader"
        );

        let mut state = self.state.write().await;
        let resp: RaftMessage = state.raft.handle_append_entries_request(
            req.leader_id, req.term
        );

        drop(state);

        match resp {
            RaftMessage::AppendEntriesResponse { term, success, from, .. } => {
                Ok(Response::new(AppendEntriesResponse {
                    node_id: from,
                    term,
                    success,
                }))
            }
            _ => Err(Status::internal("Unexpected raft message type")),
        }
    }

    async fn add_peer(
        &self,
        request: Request<AddPeerRequest>,
    ) -> Result<Response<AddPeerResponse>, Status> {
        let req = request.into_inner();

        
        let mut state = self.state.write().await;
        let peer_info = req.peer_node_info
            .ok_or_else(|| Status::invalid_argument("Missing peer_node_info"))?;
        
        let resp = state.raft.handle_add_peer_request(raft::Peer {
            node_id: peer_info.node_id.clone(),
            hostname: peer_info.hostname,
            ip_address: peer_info.ip_address,
            port: peer_info.port,
            last_seen: Instant::now(),
            status: raft::Status::Operational,
        });

        drop(state);

        tracing::info!(node_id = peer_info.node_id, "Received request to add peer node");

        match resp {
            RaftMessage::AddPeerResponse { success } => {
                Ok(Response::new(AddPeerResponse {
                    success,
                }))
            }
            _ => Err(Status::internal("Unexpected raft message type")),
        }

    }

    async fn remove_peer(
        &self,
        request: Request<RemovePeerRequest>,
    ) -> Result<Response<RemovePeerResponse>, Status> {
        let req = request.into_inner();

        
        let mut state = self.state.write().await;
        let peer_node_id = req.peer_node_id;
        
        let resp = state.raft.handle_remove_peer_request(peer_node_id.clone());

        drop(state);

        tracing::info!(node_id = peer_node_id, "Received request to remove peer node");

        match resp {
            RaftMessage::RemovePeerResponse { success } => {
                Ok(Response::new(RemovePeerResponse {
                    success,
                }))
            }
            _ => Err(Status::internal("Unexpected raft message type")),
        }
    }
}
