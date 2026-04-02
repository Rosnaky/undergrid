use std::sync::Arc;

use mesh::undergrid::{AppendEntriesRequest, AppendEntriesResponse, HeartbeatRequest, HeartbeatResponse, PingRequest, PingResponse, RegisterRequest, RegisterResponse, VoteRequest, VoteResponse, node_agent_server::NodeAgent};
use raft::RaftMessage;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::state::NodeState;


pub struct NodeAgentService {
    state: Arc<RwLock<NodeState>>,
}

impl NodeAgentService {
    pub fn new(state: Arc<RwLock<NodeState>>) -> Self {
        Self {
            state,
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
            "Registered node"
        );
        
        // Scope in closure to avoid deadlock of mutex
        {
            let mut state = self.state.write().await;
            state.raft.add_peer(raft::Peer {
                node_id: node_info.node_id,
                addr: format!("http://{}:{}", node_info.ip_address, node_info.port),
            });
        }

        // TODO: This accepts everyone for now. Change to only the leader later
        let state = self.state.read().await;

        Ok(Response::new(RegisterResponse {
            accepted: true,
            cluster_id: state.cluster_id.clone().unwrap_or_default(),
            leader_id: state.raft.node_id.clone(),
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
            RaftMessage::AppendEntriesResponse { term, success, .. } => {
                Ok(Response::new(AppendEntriesResponse {
                    term,
                    success,
                }))
            }
            _ => Err(Status::internal("Unexpected raft message type")),
        }
    }
}
