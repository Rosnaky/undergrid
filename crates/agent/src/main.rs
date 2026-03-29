use agent::{config::config::NodeConfig, system::system::SystemSnapshot};
use tokio::signal;


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

    tracing::info!(node_id = %config.node_id, "Undergrid node starting");

    let mut interval = tokio::time::interval(
        std::time::Duration::from_secs(config.heartbeat_interval_secs)
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let system_snapshot: SystemSnapshot = match SystemSnapshot::collect() {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        tracing::error!("Failed to get system snapshot: {}", e);
                        break;
                    }
                };

                tracing::info!(
                    cpu_cores = system_snapshot.cpu.cpu_cores,
                    mem_used_mb = (system_snapshot.memory.memory_total_bytes - system_snapshot.memory.memory_available_bytes) / 1_048_576,
                    mem_total_mb = system_snapshot.memory.memory_total_bytes / 1_048_576,
                    "heartbeat"
                );
            }
            _ = signal::ctrl_c() => {
                tracing::info!("Received shutdown signal");
                break;
            }
        }
    }

    tracing::info!("Undergrid node shut down");

    Ok(())
}
