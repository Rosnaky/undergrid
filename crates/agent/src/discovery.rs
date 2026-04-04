use std::{sync::Arc, time::Instant};

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::sync::RwLock;

use crate::{client::{client::register_with_leader, client_pool::ClientPool}, state::NodeState};

pub fn advertise(node_id: &str, hostname: &str, port: u16) -> ServiceDaemon {
    let mdns = ServiceDaemon::new().expect("Failed to create mDNS daemon");

    let service_type = "_undergrid._tcp.local.";
    
    let mdns_hostname = if hostname.ends_with(".local.") {
        hostname.to_string()
    } else {
        format!("{}.local.", hostname.trim_end_matches('.'))
    };

    let service_info = ServiceInfo::new(
        service_type,
        node_id,
        &mdns_hostname,
        "127.0.0.1", // Hardcode to local network for now
        port,
        None,
    ).expect("Failed to create service info");

    mdns.register(service_info)
        .expect("Failed to register mDNS service");

    mdns
}

pub fn discover_peers(
    state: Arc<RwLock<NodeState>>,
    client_pool: Arc<ClientPool>,
) {
    let mdns = ServiceDaemon::new().expect("Failed to create mDNS browser");
    let service_type = "_undergrid._tcp.local.";
    let receiver = mdns.browse(service_type)
        .expect("Failed to browse mDNS");

    let (tx, mut rx) = tokio::sync::mpsc::channel(32);

    // Blocking thread for mDNS recv
    std::thread::spawn(move || {
        loop {
            match receiver.recv() {
                Ok(event) => {
                    if tx.blocking_send(event).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    let discovered_port = info.get_port();
                    let discovered_host = info.get_hostname().trim_end_matches('.').to_string();

                    let addr = match info.get_addresses().iter().next() {
                        Some(ip) => format!("http://{}:{}", ip, discovered_port),
                        None => continue,
                    };

                    let dominated = {
                        let s = state.read().await;
                        
                        // Skip if self
                        if info.get_fullname().contains(&s.raft.node_id) {
                            continue;
                        }

                        // Skip if already a peer
                        s.raft.is_peer_by_addr(&addr)
                    };

                    if !dominated {
                        tracing::info!(
                            host = %discovered_host,
                            port = discovered_port,
                            "Discovered new Undergrid node via mDNS"
                        );

                        match register_with_leader(&client_pool, &addr, &state).await {
                            Ok(resp) if resp.accepted => {
                                let mut s = state.write().await;
                                resp.peers.iter().for_each(|peer| {
                                    s.raft.add_peer(raft::Peer {
                                        node_id: peer.node_id.clone(),
                                        hostname: peer.hostname.clone(),
                                        ip_address: peer.ip_address.clone(),
                                        port: peer.port,
                                        last_seen: Instant::now(),
                                        status: raft::Status::Operational,
                                    });
                                });
                                tracing::info!("Auto-joined via mDNS");
                            }
                            Ok(_) => tracing::warn!("mDNS peer rejected registration"),
                            Err(e) => tracing::warn!("Failed to register with mDNS peer: {}", e),
                        }
                    }
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    tracing::info!(
                        service = %fullname,
                        "mDNS peer disappeared"
                    );
                }
                _ => {}
            }
        }
    });
}

