use std::{net::SocketAddr, sync::Arc};

use agent::{config::config::NodeConfig, server::node_agent::NodeAgentService, state::NodeState, system::system::SystemSnapshot};
use mesh::undergrid::node_agent_server::NodeAgentServer;
use tokio::{signal, sync::RwLock};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "agent=info".into()),
        )
        .init();

    // Load or create config
    let config: NodeConfig = match NodeConfig::load_or_create() {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

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
                        tracing::debug!(
                            cpu_cores = snapshot.cpu.cpu_cores,
                            "Heartbeat tick"
                        );
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
