use std::sync::Arc;

use mesh::undergrid::{HeartbeatRequest, HeartbeatResponse, NodeInfo, RegisterRequest, RegisterResponse, ResourceSnapshot, node_agent_client::NodeAgentClient};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::{client::client_error::ClientError, state::NodeState};

pub async fn client_connect(
    leader_addr: &str,
) -> Result<NodeAgentClient<Channel>, ClientError> {
    let client = NodeAgentClient::connect(leader_addr.to_string())
        .await
        .map_err(|e| ClientError::ConnectionError(e.to_string()))?;

    Ok(client)
}

pub async fn register_with_leader(
    client: &mut NodeAgentClient<tonic::transport::Channel>,
    state: &Arc<RwLock<NodeState>>
) -> Result<RegisterResponse, ClientError> {
    
    let s = state.read().await;

    let request = RegisterRequest {
        node_info: Some(NodeInfo {
            node_id: s.node_id.clone(),
            hostname: s.hostname.clone(),
            ip_address: s.bind_address.clone(),
            port: s.port as u32,
            resources: s.last_snapshot.as_ref().map(|snap| ResourceSnapshot {
                cpu_cores: snap.cpu.cpu_cores as u64,
                cpu_usage_pct: snap.cpu.cpu_usage_pct,
                memory_total_bytes: snap.memory.memory_total_bytes,
                memory_available_bytes: snap.memory.memory_available_bytes,
                disk_total_bytes: snap.disk.disk_total_bytes,
                disk_available_bytes: snap.disk.disk_available_bytes,
            }),
        }),
    };

    // Release read
    drop(s);

    let response = client.register(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;
    Ok(response.into_inner())
}

pub async fn send_heartbeat(
    client: &mut NodeAgentClient<tonic::transport::Channel>,
    state: &Arc<RwLock<NodeState>>
) -> Result<HeartbeatResponse, ClientError> {
    let s = state.read().await;

    let request = HeartbeatRequest {
        node_id: s.node_id.clone(),
        resources: s.last_snapshot.as_ref().map(|snap| ResourceSnapshot {
            cpu_cores: snap.cpu.cpu_cores as u64,
            cpu_usage_pct: snap.cpu.cpu_usage_pct,
            memory_total_bytes: snap.memory.memory_total_bytes,
            memory_available_bytes: snap.memory.memory_available_bytes,
            disk_total_bytes: snap.disk.disk_total_bytes,
            disk_available_bytes: snap.disk.disk_available_bytes,
        }),
        running_tasks: s.running_tasks.clone(),
    };

    // Release read
    drop(s);

    let response = client.heartbeat(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}
