use std::sync::Arc;

use mesh::undergrid::{HeartbeatRequest, HeartbeatResponse, PingRequest, PingResponse, RegisterRequest, RegisterResponse, node_agent_server::NodeAgent};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::state::NodeState;


pub struct NodeAgentService {
    pub state: Arc<RwLock<NodeState>>
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
            node_id: state.node_id.clone(),
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

        // TODO: This accepts everyone for now. Change to only the leader later
        let state = self.state.read().await;

        Ok(Response::new(RegisterResponse {
            accepted: true,
            cluster_id: state.cluster_id.clone().unwrap_or_default(),
            leader_id: state.leader_id.clone().unwrap_or_default(),
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
}
