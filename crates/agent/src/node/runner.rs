use std::{sync::Arc, time::Instant};

use raft::{RaftMessage, Role};
use runtime::task::TaskSpec;
use scheduler::NodeResources;
use tokio::sync::RwLock;

use crate::{
    client::{
        client_pool::ClientPool, dispatch_task, remove_peer, send_append_entries, send_vote_request,
    },
    defines::OFFLINE_TIMEOUT_MS,
    node::state::NodeState,
};

pub async fn handle_raft_tick(state: &Arc<RwLock<NodeState>>, pool: &ClientPool) {
    let role = {
        let s = state.read().await;
        s.raft.role.clone()
    };
    let should_start_election = {
        let s = state.read().await;
        s.raft.should_start_election()
    };

    if matches!(role, Role::Follower) || matches!(role, Role::Candidate) {
        if should_start_election {
            {
                let s = state.read().await;
                tracing::info!(term = s.raft.term, "Starting election");
            }
            let election_msgs = {
                let mut s = state.write().await;
                s.raft.start_election()
            };

            let mut vote_futures = vec![];
            for msg in election_msgs {
                if let RaftMessage::VoteRequest {
                    to,
                    candidate_id,
                    term,
                } = msg
                {
                    let pool = pool.clone();
                    let peer_node_id = {
                        let s = state.read().await;
                        s.raft
                            .peers
                            .iter()
                            .find(|p| p.addr() == to)
                            .map(|p| p.node_id.clone())
                            .unwrap_or(to.clone())
                    };
                    vote_futures.push(tokio::spawn(async move {
                        (
                            peer_node_id,
                            send_vote_request(&pool, &to, candidate_id, term).await,
                        )
                    }));
                }
            }

            for handle in vote_futures {
                match handle.await {
                    Ok((from, Ok(resp))) => {
                        let mut s = state.write().await;
                        let append_entries_msgs =
                            s.raft.handle_vote_response(from, resp.term, resp.granted);
                        drop(s);

                        for append_entries_msg in append_entries_msgs {
                            if let RaftMessage::AppendEntriesRequest {
                                to,
                                term,
                                leader_id,
                            } = append_entries_msg
                            {
                                let _ = send_append_entries(pool, &to, leader_id, term).await;
                            }
                        }
                    }
                    Ok((from, Err(e))) => {
                        tracing::warn!(peer = %from, "Vote request failed: {}", e);
                    }
                    Err(e) => {
                        tracing::warn!("Vote task panicked: {}", e);
                    }
                }
            }
        }
    }
    // matches!(role, Role::Follower) || matches!(role, Role::Candidate)
    else if matches!(role, Role::Leader) {
        // Check for offline nodes
        let remove_peer_msgs = {
            let mut s = state.write().await;
            s.raft
                .handle_offline_timeout(Instant::now(), OFFLINE_TIMEOUT_MS)
        };

        let mut remove_peer_futures = vec![];
        for msg in remove_peer_msgs {
            if let RaftMessage::RemovePeerRequest { to, peer_node_id } = msg {
                let pool = pool.clone();
                remove_peer_futures.push(tokio::spawn(async move {
                    (
                        to.clone(),
                        remove_peer(&pool, &to, peer_node_id.clone()).await,
                    )
                }));
            }
        }

        for handle in remove_peer_futures {
            match handle.await {
                Ok((_, Ok(resp))) => {
                    let mut s = state.write().await;
                    s.raft.handle_remove_peer_response(resp.success);
                }
                Ok((to, Err(e))) => {
                    tracing::warn!(peer = %to, "RemovePeer failed: {}", e);
                }
                Err(e) => {
                    tracing::warn!("RemovePeer task panicked: {}", e);
                }
            }
        }

        // Send heartbeat requests
        let heartbeat_msgs = {
            let s = state.read().await;
            s.raft
                .peers
                .iter()
                .map(|peer| (peer.addr().clone(), s.raft.term, s.raft.node_id.clone()))
                .collect::<Vec<_>>()
        };

        let mut heartbeat_futures = vec![];
        for (addr, term, leader_id) in heartbeat_msgs {
            let to = addr.clone();
            let pool = pool.clone();
            heartbeat_futures.push(tokio::spawn(async move {
                (
                    to.clone(),
                    send_append_entries(&pool, &to, leader_id, term).await,
                )
            }));
        }

        for handle in heartbeat_futures {
            match handle.await {
                Ok((_, Ok(resp))) => {
                    let mut s = state.write().await;
                    s.raft
                        .handle_append_entries_response(resp.node_id, resp.term, resp.success);
                }
                Ok((to, Err(e))) => {
                    tracing::warn!(peer = %to, "AppendEntries failed: {}", e);
                }
                Err(e) => {
                    tracing::warn!("AppendEntries task panicked: {}", e);
                }
            }
        }
    }
}

pub async fn handle_orchestrator_tick(state: &Arc<RwLock<NodeState>>, pool: &ClientPool) {
    let to_dispatch: Vec<(String, TaskSpec, String)> = {
        let s = state.read().await;

        if !matches!(s.raft.role, raft::Role::Leader) {
            return;
        }

        let job_count = s.orchestrator.jobs.len();
        tracing::debug!(
            "Orchestrator tick: {} jobs, {} peer resources, last_snapshot: {}",
            job_count,
            s.peer_resources.len(),
            s.last_snapshot.is_some()
        );

        let nodes: Vec<NodeResources> = {
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
        };

        let mut all_dispatches = Vec::new();

        let job_ids: Vec<String> = s.orchestrator.jobs.keys().cloned().collect();
        drop(s);

        {
            let mut s = state.write().await;
            for job_id in &job_ids {
                let dispatches = s.orchestrator.schedule_ready_tasks(job_id, &nodes);
                for (spec, node_id) in &dispatches {
                    all_dispatches.push((job_id.clone(), spec.clone(), node_id.clone()));
                }
            }
        }

        all_dispatches
    };

    for (job_id, spec, node_id) in to_dispatch {
        tracing::info!(
            job_id = %job_id,
            task_id = %spec.id,
            target_node = %node_id,
            "Dispatching task"
        );

        let addr = {
            let s = state.read().await;
            if s.raft.node_id == node_id {
                Some(format!("http://{}:{}", s.bind_address, s.port))
            } else {
                s.raft.get_peer_addr(&node_id)
            }
        };
        if let Some(addr) = addr {
            match dispatch_task(pool, &addr, job_id, spec).await {
                Ok(_) => tracing::info!("Task dispatched successfully"),
                Err(e) => tracing::error!("Dispath failed: {}", e),
            }
        } else {
            tracing::error!("No address for node {}", node_id);
        }
    }
}
