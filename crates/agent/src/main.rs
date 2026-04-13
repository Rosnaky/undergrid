use std::{net::SocketAddr, sync::Arc};

use agent::{
    client::{client_pool::ClientPool, send_heartbeat},
    config::NodeConfig,
    node::{runner::handle_orchestrator_tick, runner::handle_raft_tick, state::NodeState},
    orchestrator::Orchestrator,
    server::node_agent::NodeAgentService,
    system::SystemSnapshot,
};
use clap::Parser;
use mesh::undergrid::node_agent_server::NodeAgentServer;
use raft::Role;
use scheduler::drf::DrfScheduler;
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
    let orchestrator = Orchestrator::new(DrfScheduler);

    let state = Arc::new(RwLock::new(NodeState::new(
        config.node_id.clone(),
        system_snapshot.hostname.clone(),
        config.bind_address.clone(),
        config.port,
        orchestrator,
    )));
    let heartbeat_state = Arc::clone(&state);

    // Store initial system snapshot in state
    {
        let mut s = state.write().await;
        s.last_snapshot = Some(system_snapshot.clone());
    }

    let client_pool = Arc::new(ClientPool::new());

    // Get client if it is not a leader node
    let addr: SocketAddr = format!("{}:{}", config.bind_address, config.port)
        .parse()
        .expect("Invalid bind address");

    let _mdns =
        agent::node::discovery::advertise(&config.node_id, &system_snapshot.hostname, config.port);

    let service = NodeAgentService::new(state.clone());

    tracing::info!(node_id = %config.node_id, "Undergrid node starting");

    let grpc_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(NodeAgentServer::new(service))
            .serve(addr),
    );

    let disc_state = Arc::clone(&state);
    let disc_pool = Arc::clone(&client_pool);

    let mut raft_interval = tokio::time::interval(std::time::Duration::from_millis(10));

    let mut monitor_interval = tokio::time::interval(std::time::Duration::from_secs(5));

    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_millis(500));

    let mut orchestrator_interval = tokio::time::interval(std::time::Duration::from_millis(500));

    agent::node::discovery::discover_peers(disc_state, disc_pool);

    loop {
        tokio::select! {
            _ = monitor_interval.tick() => {
                match SystemSnapshot::collect() {
                    Ok(snapshot) => {
                        let mut s = heartbeat_state.write().await;
                        s.last_snapshot = Some(snapshot.clone());
                    },
                    Err(e) => {
                        tracing::error!("Failed to get system snapshot: {}", e);
                        break;
                    }
                };
            }
            _ = heartbeat_interval.tick() => {
                let role = {
                    let s = state.read().await;
                    s.raft.role.clone()
                };

                if matches!(role, Role::Follower) {
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
                }
            }
            _ = raft_interval.tick() => {
                handle_raft_tick(&state, &client_pool).await;
            }
            _ = signal::ctrl_c() => {
                tracing::info!("Received shutdown signal");
                break;
            }
            _ = orchestrator_interval.tick() => {
                handle_orchestrator_tick(&state, &client_pool).await;
            }
        }
    }

    grpc_handle.abort();
    tracing::info!("Undergrid node shut down");

    Ok(())
}
