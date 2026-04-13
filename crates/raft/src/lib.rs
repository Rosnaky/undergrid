use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use rand::RngExt;

#[derive(Debug, Clone)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub enum Status {
    Operational,
    Offline,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub node_id: String,
    pub hostname: String,
    pub ip_address: String,
    pub port: u32,
    pub last_seen: Instant,
    pub status: Status,
}

impl Peer {
    pub fn addr(&self) -> String {
        format!("http://{}:{}", self.ip_address, self.port)
    }
}

pub struct RaftNode {
    // Persists
    pub term: u64,
    pub voted_for: Option<String>,

    // Volatile
    pub role: Role,
    pub node_id: String,
    pub peers: Vec<Peer>,
    pub leader_id: Option<String>,

    // Election
    pub election_timeout_ms: u64,
    pub last_heartbeat: Instant,
    pub votes_received: HashSet<String>,
}

#[derive(Debug, Clone)]
pub enum RaftMessage {
    VoteRequest {
        to: String,
        candidate_id: String,
        term: u64,
    },
    VoteResponse {
        to: String,
        term: u64,
        granted: bool,
    },
    AppendEntriesRequest {
        to: String,
        term: u64,
        leader_id: String,
    },
    AppendEntriesResponse {
        to: String,
        from: String,
        term: u64,
        success: bool,
    },
    AddPeerResponse {
        success: bool,
    },
    RemovePeerRequest {
        to: String,
        peer_node_id: String,
    },
    RemovePeerResponse {
        success: bool,
    },
}

impl RaftNode {
    pub fn new(node_id: String, peers: Vec<Peer>) -> Self {
        let mut rng = rand::rng();

        Self {
            term: 0,
            voted_for: None,
            role: Role::Follower,
            node_id,
            peers,
            leader_id: None,
            election_timeout_ms: rng.random_range(150..=300),
            last_heartbeat: Instant::now(),
            votes_received: HashSet::new(),
        }
    }

    pub fn add_peer(&mut self, peer: Peer) {
        if !self.is_peer_by_node_id(&peer.node_id) && peer.node_id != self.node_id {
            self.peers.push(peer);
        }
    }

    pub fn remove_peer(&mut self, peer_node_id: &str) {
        self.peers.retain(|p| p.node_id != peer_node_id);
    }

    pub fn is_peer_by_node_id(&self, peer_node_id: &str) -> bool {
        self.peers.iter().any(|peer| peer.node_id == peer_node_id)
    }

    pub fn is_peer_by_addr(&self, addr: &str) -> bool {
        self.peers.iter().any(|peer| peer.addr() == addr)
    }

    pub fn get_peer_addr(&self, node_id: &str) -> Option<String> {
        self.peers
            .iter()
            .find(|peer| peer.node_id == node_id)
            .map(|peer| peer.addr())
    }

    pub fn handle_offline_timeout(&mut self, now: Instant, timeout_ms: u64) -> Vec<RaftMessage> {
        let offline_timeout = Duration::from_millis(timeout_ms);

        let offline_node_ids: Vec<String> = self
            .peers
            .iter()
            .filter(|peer| now.duration_since(peer.last_seen) >= offline_timeout)
            .map(|peer| peer.node_id.clone())
            .collect();

        self.peers
            .retain(|peer| now.duration_since(peer.last_seen) < offline_timeout);

        self.peers
            .iter()
            .flat_map(|peer| {
                offline_node_ids
                    .iter()
                    .map(|peer_node_id| RaftMessage::RemovePeerRequest {
                        to: peer.addr(),
                        peer_node_id: peer_node_id.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn should_start_election(&self) -> bool {
        (Instant::now() - self.last_heartbeat).as_millis() >= self.election_timeout_ms.into()
    }

    pub fn start_election(&mut self) -> Vec<RaftMessage> {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.node_id.clone());
        self.votes_received.clear();
        self.votes_received.insert(self.node_id.clone());
        self.last_heartbeat = Instant::now();
        self.reset_election_timeout();

        // Single node system
        if 1 >= self.quorum() {
            self.role = Role::Leader;
            self.leader_id = Some(self.node_id.clone());
            return vec![];
        }

        self.peers
            .iter()
            .map(|peer| RaftMessage::VoteRequest {
                to: peer.addr().clone(),
                term: self.term,
                candidate_id: self.node_id.clone(),
            })
            .collect()
    }

    pub fn handle_heartbeat_response(&mut self, node_id: String) {
        self.update_last_seen(node_id);
    }

    pub fn handle_vote_request(
        &mut self,
        candidate_id: String,
        candidate_term: u64,
    ) -> RaftMessage {
        self.update_last_seen(candidate_id.clone());

        if candidate_term < self.term {
            return RaftMessage::VoteResponse {
                to: candidate_id.clone(),
                term: self.term,
                granted: false,
            };
        }

        if candidate_term > self.term {
            self.step_down_to_follower();
            self.term = candidate_term;
            self.leader_id = None;
        }

        if self.voted_for.is_none() || self.voted_for.as_ref() == Some(&candidate_id) {
            self.voted_for = Some(candidate_id.clone());
            self.reset_election_timeout();
            self.last_heartbeat = Instant::now();
            return RaftMessage::VoteResponse {
                to: candidate_id.clone(),
                term: self.term,
                granted: true,
            };
        }

        RaftMessage::VoteResponse {
            to: candidate_id.clone(),
            term: self.term,
            granted: false,
        }
    }

    pub fn handle_vote_response(
        &mut self,
        from_id: String,
        term: u64,
        granted: bool,
    ) -> Vec<RaftMessage> {
        self.update_last_seen(from_id.clone());

        if !matches!(self.role, Role::Candidate) {
            return vec![];
        }

        if term > self.term {
            self.term = term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.votes_received.clear();
            return vec![];
        }

        if !granted {
            return vec![];
        }

        self.votes_received.insert(from_id.clone());
        if self.votes_received.len() >= self.quorum() {
            self.role = Role::Leader;
            self.leader_id = Some(self.node_id.clone());

            return self
                .peers
                .iter()
                .map(|peer| RaftMessage::AppendEntriesRequest {
                    to: peer.addr().clone(),
                    term: self.term,
                    leader_id: self.node_id.clone(),
                })
                .collect();
        }

        vec![]
    }

    pub fn handle_append_entries_request(&mut self, leader_id: String, term: u64) -> RaftMessage {
        self.update_last_seen(leader_id.clone());

        if term < self.term {
            return RaftMessage::AppendEntriesResponse {
                to: leader_id.clone(),
                from: self.node_id.clone(),
                term: self.term,
                success: false,
            };
        }

        self.step_down_to_follower();
        self.term = term;
        self.leader_id = Some(leader_id.clone());

        RaftMessage::AppendEntriesResponse {
            to: leader_id.clone(),
            from: self.node_id.clone(),
            term,
            success: true,
        }
    }

    pub fn handle_append_entries_response(&mut self, node_id: String, term: u64, success: bool) {
        self.update_last_seen(node_id);
        let _ = success;
        if term > self.term {
            self.term = term;
            self.step_down_to_follower();
        }
    }

    pub fn handle_add_peer_request(&mut self, peer: Peer) -> RaftMessage {
        self.add_peer(peer);

        RaftMessage::AddPeerResponse { success: true }
    }

    pub fn handle_add_peer_response(&mut self, success: bool) {
        let _ = success;
    }

    pub fn handle_remove_peer_request(&mut self, peer_node_id: String) -> RaftMessage {
        self.remove_peer(&peer_node_id);

        RaftMessage::RemovePeerResponse { success: true }
    }

    pub fn handle_remove_peer_response(&mut self, success: bool) {
        let _ = success;
    }

    pub fn quorum(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    // Private functions

    fn step_down_to_follower(&mut self) {
        self.role = Role::Follower;
        self.voted_for = None;
        self.last_heartbeat = Instant::now();
        self.reset_election_timeout();
    }

    fn reset_election_timeout(&mut self) {
        let mut rng = rand::rng();

        self.election_timeout_ms = rng.random_range(150..=300);
    }

    fn update_last_seen(&mut self, node_id: String) {
        if let Some(peer) = self.peers.iter_mut().find(|p| p.node_id == node_id) {
            peer.last_seen = Instant::now();
        }
    }
}
