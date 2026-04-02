use std::{net::SocketAddr, sync::Arc};

use agent::{client::{client::{register_with_leader, send_append_entries, send_heartbeat, send_vote_request}, client_pool::ClientPool}, config::config::NodeConfig, server::node_agent::NodeAgentService, state::NodeState, system::system::SystemSnapshot};
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
    join: Option<String>
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
    let mut config: NodeConfig = match NodeConfig::load_or_create() {
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
    
    // Store initial system snapshot in state
    {
        let mut s = state.write().await;
        s.last_snapshot = Some(system_snapshot);
    }

    let client_pool = Arc::new(ClientPool::new());

    // Get client if it is not a leader node
    if let Some(ref addr) = args.join {
        let full_addr = format!("http://{}", addr);
        match register_with_leader(&client_pool, &full_addr, &state).await {
            Ok(resp) => {
                if resp.accepted {
                    let mut s = state.write().await;
                    s.cluster_id = Some(resp.cluster_id);
                    s.raft.add_peer(raft::Peer {
                        node_id: resp.leader_id,
                        addr: full_addr,
                    });
                    tracing::info!(node_id = &config.node_id, "Registered with leader");
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

    let raft_state = Arc::clone(&state);
    let raft_pool = Arc::clone(&client_pool);
    let raft_handle = tokio::spawn(async move {
        let mut tick_interval = tokio::time::interval(
            std::time::Duration::from_millis(10),
        );

        loop {
            tick_interval.tick().await;

            let election_msgs = {
                let s = raft_state.read().await;
                let should_elect = matches!(s.raft.role, Role::Follower | Role::Candidate)
                    && s.raft.should_start_election();
                drop(s);

                if should_elect {
                    let mut s = raft_state.write().await;
                    s.raft.start_election()
                } else {
                    vec![]
                }
            };

            if !election_msgs.is_empty() {
                let mut vote_futures = vec![];
                for msg in election_msgs {
                    if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
                        let pool = Arc::clone(&raft_pool);
                        let peer_node_id = {
                            let s = raft_state.read().await;
                            s.raft.peers.iter()
                                .find(|p| p.addr == to)
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
                            let mut s = raft_state.write().await;
                            let follow_up = s.raft.handle_vote_response(
                                from, resp.term, resp.granted,
                            );
                            drop(s);

                            for msg in follow_up {
                                if let RaftMessage::AppendEntriesRequest { to, term, leader_id } = msg {
                                    let _ = send_append_entries(&raft_pool, &to, leader_id, term).await;
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

            let heartbeat_msgs = {
                let s = raft_state.read().await;
                if matches!(s.raft.role, Role::Leader) {
                    s.raft.peers.iter()
                        .map(|peer| (peer.addr.clone(), s.raft.term, s.raft.node_id.clone()))
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                }
            };

            if !heartbeat_msgs.is_empty() {
                let mut heartbeat_futures = vec![];
                for (addr, term, leader_id) in heartbeat_msgs {
                    let to = addr.clone();
                    let pool = Arc::clone(&raft_pool);
                    heartbeat_futures.push(tokio::spawn(async move {
                        (to.clone(), send_append_entries(&pool, &to, leader_id, term).await)
                    }));
                }

                for handle in heartbeat_futures {
                    match handle.await {
                        Ok((_, Ok(resp))) => {
                            let mut s = raft_state.write().await;
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
    });

    let monitor_state = Arc::clone(&state);
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(5),
        );

        loop {
            interval.tick().await;

            match SystemSnapshot::collect() {
                Ok(snapshot) => {
                    let mut s = monitor_state.write().await;
                    s.last_snapshot = Some(snapshot);
                }
                Err(e) => {
                    tracing::warn!("Failed to collect snapshot: {}", e);
                }
            }
        }
    });

    // ── Main just waits for shutdown ────────────────────
    signal::ctrl_c().await?;
    tracing::info!("Received shutdown signal");

    raft_handle.abort();
    monitor_handle.abort();
    grpc_handle.abort();
    tracing::info!("Undergrid node shut down");

    Ok(())
}
