

use mesh::undergrid::node_agent_client::NodeAgentClient;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use std::collections::HashMap;

use crate::client::client_error::ClientError;


pub struct ClientPool {
    cache: RwLock<HashMap<String, NodeAgentClient<Channel>>>,
}

impl ClientPool {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, addr: &str) -> Result<NodeAgentClient<Channel>, ClientError> {
        {
            let cache = self.cache.read().await;
            if let Some(client) = cache.get(addr) {
                return Ok(client.clone()); // tonic clients are cheap to clone
            }
        }

        let client = NodeAgentClient::connect(addr.to_string())
            .await
            .map_err(|e| ClientError::ConnectionError(e.to_string()))?;

        let mut cache = self.cache.write().await;
        cache.insert(addr.to_string(), client.clone());

        Ok(client)
    }
}