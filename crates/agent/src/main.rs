use std::{net::SocketAddr, sync::Arc};

use agent::{client::client::{client_connect, register_with_leader, send_heartbeat}, config::config::NodeConfig, server::node_agent::NodeAgentService, state::NodeState, system::system::SystemSnapshot};
use clap::Parser;
use mesh::undergrid::node_agent_server::NodeAgentServer;
use tokio::{signal, sync::RwLock};

#[derive(Parser)]
#[command(name = "agent")]
#[command(about = "Undergrid Node Agent")]
struct Args {
    #[arg(long, default_value_t = 7070)]
    port: u16,
    #[arg(long)]
    init: bool, // Remove when Raft Consensus algorithm is implemented for leader election
    #[arg(long)]
    join: Option<String>,
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
    let heartbeat_state = Arc::clone(&state);

    // Store initial system snapshot in state
    {
        let mut s = state.write().await;
        s.last_snapshot = Some(system_snapshot);
    }

    // Get client if it is not a leader node
    let mut client = match args.join {
        Some(ref addr) => {
            let full_addr = format!("http://{}", addr);
            let mut c = match client_connect(&full_addr).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to connect to leader: {}", e);
                    std::process::exit(1);
                }
            };
            
            match register_with_leader(&mut c, &state).await {
                Ok(resp) => {
                    if resp.accepted {
                        let mut s = state.write().await;
                        s.cluster_id = Some(resp.cluster_id);
                        s.leader_id = Some(resp.leader_id);
                        tracing::info!(node_id = &config.node_id, "Registered with leader");
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to register: {}", e);
                    std::process::exit(1);
                }
            }

            Some(c)
        }
        None => None,
    };

    let addr: SocketAddr = format!("{}:{}", config.bind_address, config.port)
        .parse()
        .expect("Invalid bind address");

    let service = NodeAgentService::new(state);

    tracing::info!(node_id = %config.node_id, "Undergrid node starting");

    let grpc_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(NodeAgentServer::new(service))
            .serve(addr)
    );

    let mut interval = tokio::time::interval(
        std::time::Duration::from_secs(config.heartbeat_interval_secs)
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match SystemSnapshot::collect() {
                    Ok(snapshot) => {
                        let mut s = heartbeat_state.write().await;
                        s.last_snapshot = Some(snapshot.clone());
                        drop(s);

                        if let Some(ref mut c) = client {
                            match send_heartbeat(c, &heartbeat_state).await {
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
                    },
                    Err(e) => {
                        tracing::error!("Failed to get system snapshot: {}", e);
                        break;
                    }
                };
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
