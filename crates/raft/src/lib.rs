use std::{collections::HashSet, time::Instant};

use rand::RngExt;

#[derive(Debug, Clone)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub node_id: String,
    pub addr: String,
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
        term: u64,
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
        self.peers.push(peer);
    }

    pub fn should_start_election(&self) -> bool {
        (Instant::now() - self.last_heartbeat).as_millis() >= self.election_timeout_ms.into()
    }

    pub fn start_election(&mut self) -> Vec<RaftMessage> {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.node_id.clone());
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
                to: peer.addr.clone(),
                term: self.term, 
                candidate_id: self.node_id.clone(), 
            })
            .collect()
    }

    pub fn handle_vote_request(&mut self, candidate_id: String, candidate_term: u64) -> RaftMessage {
        if candidate_term < self.term {
            return RaftMessage::VoteResponse { 
                to: candidate_id.clone(), 
                term: self.term, 
                granted: false 
            }
        }

        if candidate_term > self.term {
            self.term = candidate_term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;
        }
        
        if self.voted_for.is_none() ||
            self.voted_for.as_ref() == Some(&candidate_id) 
        {
            self.voted_for = Some(candidate_id.clone());
            self.reset_election_timeout();
            self.last_heartbeat = Instant::now();
            return RaftMessage::VoteResponse { 
                to: candidate_id.clone(), 
                term: self.term, 
                granted: true,
            }
        }

        RaftMessage::VoteResponse { 
            to: candidate_id.clone(), 
            term: self.term, 
            granted: false 
        }
    }

    pub fn handle_vote_response(&mut self, from_id: String, term: u64, granted: bool) -> Vec<RaftMessage> {
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

            return self.peers
                .iter()
                .map(|peer| RaftMessage::AppendEntriesRequest { 
                    to: peer.addr.clone(), 
                    term: self.term, 
                    leader_id: self.node_id.clone(),
                })
                .collect()
        }

        return vec![];
    }

    pub fn handle_append_entries_request(&mut self, leader_id: String, term: u64) -> RaftMessage {
        if term < self.term
        {
            return RaftMessage::AppendEntriesResponse { 
                to: leader_id.clone(), 
                term: self.term, 
                success: false,
            }
        }

        self.term = term;
        self.role = Role::Follower;
        self.leader_id = Some(leader_id.clone());
        self.voted_for = None;
        self.last_heartbeat = Instant::now();
        self.reset_election_timeout();

        RaftMessage::AppendEntriesResponse { 
            to: leader_id.clone(), 
            term, 
            success: true 
        }
    }

    pub fn handle_append_entries_response(&mut self, term: u64, success: bool) {
        let _ = success;
        if term > self.term {
            self.term = term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.last_heartbeat = Instant::now();
            self.reset_election_timeout();
        }
    }

    pub fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }
        
    // Private functions

    fn reset_election_timeout(&mut self) {
        let mut rng = rand::rng();

        self.election_timeout_ms = rng.random_range(150..=300);
    }
}
