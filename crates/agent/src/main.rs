use std::{net::SocketAddr, sync::Arc, time::Instant};

use agent::{client::{client::{register_with_leader, send_append_entries, send_heartbeat, send_vote_request}, client_pool::ClientPool}, config::config::NodeConfig, defines::OFFLINE_TIMEOUT_MS, server::node_agent::NodeAgentService, state::NodeState, system::system::SystemSnapshot};
use clap::Parser;
use mesh::undergrid::node_agent_server::NodeAgentServer;
use raft::{RaftMessage, Role};
use tokio::{signal, sync::RwLock};

#[derive(Parser)]
#[command(name = "agent")]
#[command(about = "Undergrid Node Agent")]
struct Args {
    #[arg(long, default_value_t = 7070)]
    port: u16,

    #[arg(long)]
    join_hostname: Option<String>,
    
    #[arg(long)]
    join_port: Option<u16>,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    let args = Args::parse();

    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "agent=info".into()),
        )
        .init();

    // Load or create config
    let mut config: NodeConfig = match NodeConfig::load_or_create(args.port) {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // Override port from argument
    config.port = args.port;

    let system_snapshot: SystemSnapshot = match SystemSnapshot::collect() {
        Ok(snapshot) => snapshot,
        Err(e) => {
            tracing::error!("Failed to get system snapshot: {}", e);
            std::process::exit(1);
        }
    };
    SystemSnapshot::display(&system_snapshot);

    let state = Arc::new(RwLock::new(NodeState::new(
        config.node_id.clone(),
        system_snapshot.hostname.clone(),
        config.bind_address.clone(),
        config.port,
    )));
    let heartbeat_state = Arc::clone(&state);

    // Store initial system snapshot in state
    {
        let mut s = state.write().await;
        s.last_snapshot = Some(system_snapshot);
    }

    let client_pool = Arc::new(ClientPool::new());

    // Get client if it is not a leader node
    if let Some(ref hostname) = args.join_hostname && let Some(port) = args.join_port {
        let full_addr = format!("http://{}:{}", hostname, port);
        match register_with_leader(&client_pool, &full_addr, &state).await {
            Ok(resp) => {
                if resp.accepted {
                    let mut s = state.write().await;
                    s.cluster_id = Some(resp.cluster_id);
                    s.raft.add_peer(raft::Peer {
                        node_id: resp.leader_id.clone(),
                        hostname: hostname.clone(),
                        ip_address: hostname.clone(),
                        port: port as u32,
                        last_seen: Instant::now(),
                        status: raft::Status::Operational,
                    });

                    for peer_info in resp.peers {
                        if peer_info.node_id == config.node_id || peer_info.node_id == resp.leader_id {
                            continue;
                        }

                        tracing::info!(node_id = peer_info.node_id, "Added peer");

                        s.raft.add_peer(raft::Peer {
                            node_id: peer_info.node_id,
                            hostname: peer_info.hostname,
                            ip_address: peer_info.ip_address,
                            port: peer_info.port,
                            last_seen: Instant::now(),
                            status: raft::Status::Operational,
                        });
                    }

                    tracing::info!(node_id = resp.leader_id, "Registered with leader");
                }
            }
            Err(e) => {
                tracing::error!("Failed to register: {}", e);
                std::process::exit(1);
            }
        }
    };

    let addr: SocketAddr = format!("{}:{}", config.bind_address, config.port)
        .parse()
        .expect("Invalid bind address");

    let service = NodeAgentService::new(state.clone());

    tracing::info!(node_id = %config.node_id, "Undergrid node starting");

    let grpc_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(NodeAgentServer::new(service))
            .serve(addr)
    );

    let mut interval = tokio::time::interval(
        std::time::Duration::from_millis(100),
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match SystemSnapshot::collect() {
                    Ok(snapshot) => {
                        let mut s = heartbeat_state.write().await;
                        s.last_snapshot = Some(snapshot.clone());
                        drop(s);

                        {
                            
                            let leader_addr = {
                                let s = state.read().await;
                                s.raft.leader_id.as_ref().and_then(|lid| {
                                    s.raft.peers.iter().find(|p| &p.node_id == lid).map(|p| p.addr().clone())
                                })
                            };

                            if let Some(addr) = leader_addr {
                                match send_heartbeat(&client_pool, &addr, &heartbeat_state).await {
                                    Ok(_) => tracing::debug!("Heartbeat sent"),
                                    Err(e) => tracing::warn!("Heartbeat failed: {}", e),
                                }
                            }
                            else {
                                tracing::debug!(
                                    cpu_cores = snapshot.cpu.cpu_cores,
                                    "Heartbeat tick (local)"
                                )
                            }
                        }
                    },
                    Err(e) => {
                        tracing::error!("Failed to get system snapshot: {}", e);
                        break;
                    }
                };

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
                            tracing::info!(node_id = s.raft.node_id.clone(), term = s.raft.term, "Starting election");
                        }
                        let election_msgs = {
                            let mut s = state.write().await;
                            s.raft.start_election()
                        };

                        let mut vote_futures = vec![];
                        for msg in election_msgs {
                            if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
                                let pool = Arc::clone(&client_pool);
                                let peer_node_id = {
                                    let s = state.read().await;
                                    s.raft.peers.iter()
                                        .find(|p| p.addr() == to)
                                        .map(|p| p.node_id.clone())
                                        .unwrap_or(to.clone())
                                };
                                vote_futures.push(tokio::spawn(async move {
                                    (peer_node_id, send_vote_request(&pool, &to, candidate_id, term).await)
                                }));
                            }
                        }

                        for handle in vote_futures {
                            match handle.await {
                                Ok((from, Ok(resp))) => {
                                    let mut s = state.write().await;
                                    let append_entries_msgs = s.raft.handle_vote_response(
                                        from,
                                        resp.term,
                                        resp.granted,
                                    );
                                    drop(s);
                                    {
                                        let s = state.read().await;
                                        tracing::info!(node_id = s.raft.node_id.clone(), is_elected = resp.granted, "Election results");
                                    }


                                    for append_entries_msg in append_entries_msgs {
                                        if let RaftMessage::AppendEntriesRequest { to, term, leader_id } = append_entries_msg {
                                            let _ = send_append_entries(&client_pool, &to, leader_id, term).await;
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
                } // matches!(role, Role::Follower) || matches!(role, Role::Candidate)
                else if matches!(role, Role::Leader) {
                    // Check for offline nodes
                    let mut s = state.write().await;
                    let now = Instant::now();
                    
                    s.raft.handle_offline_timeout(now, OFFLINE_TIMEOUT_MS);

                    // Send heartbeat requests
                    let heartbeat_msgs = {
                        let s = state.read().await;
                        s.raft.peers
                            .iter()
                            .map(|peer| (peer.addr().clone(), s.raft.term, s.raft.node_id.clone()))
                            .collect::<Vec<_>>()
                    };

                    let mut heartbeat_futures = vec![];
                    for (addr, term, leader_id) in heartbeat_msgs {
                        let to = addr.clone();
                        let pool = Arc::clone(&client_pool);
                        heartbeat_futures.push(tokio::spawn(async move {
                            (to.clone(), send_append_entries(&pool, &to, leader_id, term).await)
                        }));
                    }

                    for handle in heartbeat_futures {
                        match handle.await {
                            Ok((_, Ok(resp))) => {
                                let mut s = state.write().await;
                                s.raft.handle_append_entries_response(resp.term, resp.success);
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
            _ = signal::ctrl_c() => {
                tracing::info!("Received shutdown signal");
                break;
            }
        }
    }

    grpc_handle.abort();
    tracing::info!("Undergrid node shut down");

    Ok(())
}
