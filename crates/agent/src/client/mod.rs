pub mod client_error;
pub mod client_pool;

use std::sync::Arc;

use mesh::undergrid::{
    AddPeerRequest, AddPeerResponse, AppendEntriesRequest, AppendEntriesResponse,
    DispatchTaskRequest, DispatchTaskResponse, HeartbeatRequest, HeartbeatResponse, NodeInfo,
    RegisterRequest, RegisterResponse, RemovePeerRequest, RemovePeerResponse,
    ReportTaskResultRequest, ReportTaskResultResponse, ResourceSnapshot, SubmitJobRequest,
    SubmitJobResponse, VoteRequest, VoteResponse,
};
use runtime::{
    job::JobId,
    task::{TaskId, TaskOutput, TaskSpec},
};
use tokio::sync::RwLock;

use crate::{
    client::{client_error::ClientError, client_pool::ClientPool},
    node::state::NodeState,
};

pub async fn register_with_leader(
    pool: &ClientPool,
    addr: &str,
    state: &Arc<RwLock<NodeState>>,
) -> Result<RegisterResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let s = state.read().await;

    let request = RegisterRequest {
        node_info: Some(NodeInfo {
            node_id: s.raft.node_id.clone(),
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

    let response = client
        .register(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;
    Ok(response.into_inner())
}

pub async fn send_heartbeat(
    pool: &ClientPool,
    addr: &str,
    state: &Arc<RwLock<NodeState>>,
) -> Result<HeartbeatResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let s = state.read().await;

    let request = HeartbeatRequest {
        node_id: s.raft.node_id.clone(),
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

    let response = client
        .heartbeat(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

pub async fn send_vote_request(
    pool: &ClientPool,
    addr: &str,
    candidate_id: String,
    term: u64,
) -> Result<VoteResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let request = VoteRequest { term, candidate_id };

    let response = client
        .vote(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

pub async fn send_append_entries(
    pool: &ClientPool,
    addr: &str,
    leader_id: String,
    term: u64,
) -> Result<AppendEntriesResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let request = AppendEntriesRequest { term, leader_id };

    let response = client
        .append_entries(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

pub async fn add_peer(
    pool: &ClientPool,
    addr: &str,
    peer_node_info: NodeInfo,
) -> Result<AddPeerResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let request = AddPeerRequest {
        peer_node_info: Some(peer_node_info),
    };

    let response = client
        .add_peer(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

pub async fn remove_peer(
    pool: &ClientPool,
    addr: &str,
    peer_node_id: String,
) -> Result<RemovePeerResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let request = RemovePeerRequest { peer_node_id };

    let response = client
        .remove_peer(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

pub async fn submit_job(
    pool: &ClientPool,
    addr: &str,
    job_id: JobId,
    task_specs: Vec<TaskSpec>,
) -> Result<SubmitJobResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let mesh_task_specs = task_specs
        .into_iter()
        .map(mesh::undergrid::TaskSpec::from)
        .collect();

    let request = SubmitJobRequest {
        job_id,
        tasks: mesh_task_specs,
    };

    let response = client
        .submit_job(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

pub async fn dispatch_task(
    pool: &ClientPool,
    addr: &str,
    job_id: JobId,
    task_spec: TaskSpec,
) -> Result<DispatchTaskResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let mesh_task_spec = mesh::undergrid::TaskSpec::from(task_spec);

    let request = DispatchTaskRequest {
        job_id,
        task_spec: Some(mesh_task_spec),
    };

    let response = client
        .dispatch_task(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}

#[allow(clippy::too_many_arguments)]
pub async fn report_task_result(
    pool: &ClientPool,
    addr: &str,
    node_id: String,
    job_id: JobId,
    task_id: TaskId,
    output: TaskOutput,
    executed: bool,
    error: String,
) -> Result<ReportTaskResultResponse, ClientError> {
    let mut client = pool.get(addr).await?;

    let mesh_output = mesh::undergrid::TaskOutput::from(output);

    let request = ReportTaskResultRequest {
        node_id,
        job_id,
        task_id,
        output: Some(mesh_output),
        executed,
        error,
    };

    let response = client
        .report_task_result(request)
        .await
        .map_err(|e| ClientError::QueryError(e.to_string()))?;

    Ok(response.into_inner())
}
