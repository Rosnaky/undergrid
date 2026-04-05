use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use raft::{Peer, RaftMessage, RaftNode, Role, Status};

// ── Helpers ──────────────────────────────────────────────

fn peer(id: &str, ip: &str, port: u32) -> Peer {
    Peer {
        node_id: id.to_string(),
        hostname: id.to_string(),
        ip_address: ip.to_string(),
        port,
        last_seen: Instant::now(),
        status: Status::Operational,
    }
}

fn node(id: &str, peers: Vec<Peer>) -> RaftNode {
    RaftNode::new(id.to_string(), peers)
}

fn three_nodes() -> (RaftNode, RaftNode, RaftNode) {
    (
        node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]),
        node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]),
        node("C", vec![peer("A", "A", 0), peer("B", "B", 1)]),
    )
}

fn five_nodes() -> (RaftNode, RaftNode, RaftNode, RaftNode, RaftNode) {
    (
        node(
            "A",
            vec![
                peer("B", "B", 1),
                peer("C", "C", 2),
                peer("D", "D", 3),
                peer("E", "E", 4),
            ],
        ),
        node(
            "B",
            vec![
                peer("A", "A", 0),
                peer("C", "C", 2),
                peer("D", "D", 3),
                peer("E", "E", 4),
            ],
        ),
        node(
            "C",
            vec![
                peer("A", "A", 0),
                peer("B", "B", 1),
                peer("D", "D", 3),
                peer("E", "E", 4),
            ],
        ),
        node(
            "D",
            vec![
                peer("A", "A", 0),
                peer("B", "B", 1),
                peer("C", "C", 2),
                peer("E", "E", 4),
            ],
        ),
        node(
            "E",
            vec![
                peer("A", "A", 0),
                peer("B", "B", 1),
                peer("C", "C", 2),
                peer("D", "D", 3),
            ],
        ),
    )
}

fn is_follower(n: &RaftNode) -> bool {
    matches!(n.role, Role::Follower)
}
fn is_candidate(n: &RaftNode) -> bool {
    matches!(n.role, Role::Candidate)
}
fn is_leader(n: &RaftNode) -> bool {
    matches!(n.role, Role::Leader)
}

/// Simulate A winning election in a 3-node cluster
fn elect_leader_3(a: &mut RaftNode, b: &mut RaftNode, c: &mut RaftNode) {
    let vote_requests = a.start_election();
    for msg in vote_requests {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
        {
            let (voter_id, resp) = if to == "http://B:1" {
                ("B".to_string(), b.handle_vote_request(candidate_id, term))
            } else {
                ("C".to_string(), c.handle_vote_request(candidate_id, term))
            };
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                a.handle_vote_response(voter_id, term, granted);
            }
        }
    }
}

/// Send one round of heartbeats from leader A to B and C
fn send_heartbeats_3(a: &mut RaftNode, b: &mut RaftNode, c: &mut RaftNode) {
    let heartbeats: Vec<_> = a
        .peers
        .iter()
        .map(|p| (p.addr(), a.term, a.node_id.clone()))
        .collect();
    for (to, term, leader_id) in heartbeats {
        let resp = if to == "http://B:1" {
            b.handle_append_entries_request(leader_id, term)
        } else {
            c.handle_append_entries_request(leader_id, term)
        };
        if let RaftMessage::AppendEntriesResponse {
            term,
            success,
            from,
            ..
        } = resp
        {
            a.handle_append_entries_response(from, term, success);
        }
    }
}

// ═══════════════════════════════════════════════════════════
// ELECTION BASICS
// ═══════════════════════════════════════════════════════════

#[test]
fn new_node_starts_as_follower() {
    let n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    assert!(is_follower(&n));
    assert_eq!(n.term, 0);
    assert!(n.voted_for.is_none());
    assert!(n.leader_id.is_none());
}

#[test]
fn start_election_transitions_to_candidate() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    let msgs = n.start_election();
    assert!(is_candidate(&n));
    assert_eq!(n.term, 1);
    assert_eq!(n.voted_for, Some("A".to_string()));
    assert!(n.votes_received.contains("A"));
    assert_eq!(n.votes_received.len(), 1);
    assert_eq!(msgs.len(), 2);
}

#[test]
fn single_node_wins_election_immediately() {
    let mut n = node("A", vec![]);
    let msgs = n.start_election();
    assert!(is_leader(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
    assert!(msgs.is_empty());
}

#[test]
fn start_election_sends_correct_vote_requests() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    let msgs = n.start_election();
    for msg in &msgs {
        match msg {
            RaftMessage::VoteRequest {
                term, candidate_id, ..
            } => {
                assert_eq!(*term, 1);
                assert_eq!(candidate_id, "A");
            }
            _ => panic!("Expected VoteRequest"),
        }
    }
    let targets: HashSet<String> = msgs
        .iter()
        .map(|m| match m {
            RaftMessage::VoteRequest { to, .. } => to.clone(),
            _ => unreachable!(),
        })
        .collect();
    assert!(targets.contains("http://B:1"));
    assert!(targets.contains("http://C:2"));
}

#[test]
fn election_clears_previous_votes() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    // Simulate getting a vote
    n.votes_received.insert("B".to_string());
    assert_eq!(n.votes_received.len(), 2);

    // Start new election — must clear old votes
    n.start_election();
    assert_eq!(n.term, 2);
    assert_eq!(n.votes_received.len(), 1);
    assert!(n.votes_received.contains("A"));
    assert!(!n.votes_received.contains("B"));
}

#[test]
fn consecutive_elections_increment_term() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    assert_eq!(n.term, 1);
    n.start_election();
    assert_eq!(n.term, 2);
    n.start_election();
    assert_eq!(n.term, 3);
}

// ═══════════════════════════════════════════════════════════
// VOTE GRANTING
// ═══════════════════════════════════════════════════════════

#[test]
fn follower_grants_vote_to_first_candidate() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    let resp = n.handle_vote_request("A".to_string(), 1);
    match resp {
        RaftMessage::VoteResponse { granted, term, .. } => {
            assert!(granted);
            assert_eq!(term, 1);
        }
        _ => panic!("Expected VoteResponse"),
    }
    assert_eq!(n.voted_for, Some("A".to_string()));
    assert_eq!(n.term, 1);
}

#[test]
fn follower_rejects_second_candidate_same_term() {
    let mut n = node("C", vec![peer("A", "A", 0), peer("B", "B", 1)]);
    n.handle_vote_request("A".to_string(), 1);
    let resp = n.handle_vote_request("B".to_string(), 1);
    match resp {
        RaftMessage::VoteResponse { granted, .. } => assert!(!granted),
        _ => panic!("Expected VoteResponse"),
    }
    assert_eq!(n.voted_for, Some("A".to_string()));
}

#[test]
fn follower_grants_vote_again_to_same_candidate() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.handle_vote_request("A".to_string(), 1);
    let resp = n.handle_vote_request("A".to_string(), 1);
    match resp {
        RaftMessage::VoteResponse { granted, .. } => assert!(granted),
        _ => panic!("Expected VoteResponse"),
    }
}

#[test]
fn reject_vote_for_stale_term() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.term = 5;
    let resp = n.handle_vote_request("A".to_string(), 3);
    match resp {
        RaftMessage::VoteResponse { granted, term, .. } => {
            assert!(!granted);
            assert_eq!(term, 5);
        }
        _ => panic!("Expected VoteResponse"),
    }
}

#[test]
fn higher_term_vote_request_causes_step_down() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("B".to_string());
    let resp = n.handle_vote_request("A".to_string(), 5);
    match resp {
        RaftMessage::VoteResponse { granted, term, .. } => {
            assert!(granted);
            assert_eq!(term, 5);
        }
        _ => panic!("Expected VoteResponse"),
    }
    assert!(is_follower(&n));
    assert_eq!(n.term, 5);
    assert_eq!(n.voted_for, Some("A".to_string()));
}

#[test]
fn vote_request_from_higher_term_clears_leader_id() {
    let mut n = node("B", vec![peer("A", "A", 0)]);
    n.term = 1;
    n.leader_id = Some("A".to_string());

    n.handle_vote_request("A".to_string(), 3);

    assert!(n.leader_id.is_none());
    assert_eq!(n.term, 3);
}

// ═══════════════════════════════════════════════════════════
// WINNING ELECTIONS
// ═══════════════════════════════════════════════════════════

#[test]
fn candidate_becomes_leader_with_majority() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    let msgs = n.handle_vote_response("B".to_string(), 1, true);
    assert!(is_leader(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
    assert_eq!(msgs.len(), 2);
}

#[test]
fn candidate_does_not_become_leader_without_majority() {
    let mut n = node(
        "A",
        vec![
            peer("B", "B", 1),
            peer("C", "C", 2),
            peer("D", "D", 3),
            peer("E", "E", 4),
        ],
    );
    n.start_election();
    let msgs = n.handle_vote_response("B".to_string(), 1, true);
    assert!(is_candidate(&n));
    assert!(msgs.is_empty());
}

#[test]
fn candidate_steps_down_on_higher_term_in_vote_response() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    let msgs = n.handle_vote_response("B".to_string(), 5, false);
    assert!(is_follower(&n));
    assert_eq!(n.term, 5);
    assert!(n.voted_for.is_none());
    assert!(n.votes_received.is_empty());
    assert!(msgs.is_empty());
}

#[test]
fn duplicate_vote_from_same_node_does_not_double_count() {
    let mut n = node(
        "A",
        vec![
            peer("B", "B", 1),
            peer("C", "C", 2),
            peer("D", "D", 3),
            peer("E", "E", 4),
        ],
    );
    n.start_election();
    n.handle_vote_response("B".to_string(), 1, true);
    let msgs = n.handle_vote_response("B".to_string(), 1, true);
    assert!(is_candidate(&n));
    assert!(msgs.is_empty());
}

#[test]
fn non_candidate_ignores_vote_response() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    let msgs = n.handle_vote_response("B".to_string(), 1, true);
    assert!(is_follower(&n));
    assert!(msgs.is_empty());
}

#[test]
fn leader_ignores_stale_vote_response() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    n.handle_vote_response("B".to_string(), 1, true);
    assert!(is_leader(&n));
    let msgs = n.handle_vote_response("C".to_string(), 1, true);
    assert!(is_leader(&n));
    assert!(msgs.is_empty());
}

#[test]
fn rejected_vote_does_not_change_state() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    let msgs = n.handle_vote_response("B".to_string(), 1, false);
    assert!(is_candidate(&n));
    assert_eq!(n.votes_received.len(), 1);
    assert!(msgs.is_empty());
}

// ═══════════════════════════════════════════════════════════
// APPEND ENTRIES
// ═══════════════════════════════════════════════════════════

#[test]
fn follower_accepts_append_entries_from_leader() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    let resp = n.handle_append_entries_request("A".to_string(), 1);
    match resp {
        RaftMessage::AppendEntriesResponse { success, term, .. } => {
            assert!(success);
            assert_eq!(term, 1);
        }
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert!(is_follower(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
}

#[test]
fn reject_append_entries_from_stale_term() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.term = 5;
    let resp = n.handle_append_entries_request("A".to_string(), 3);
    match resp {
        RaftMessage::AppendEntriesResponse { success, term, .. } => {
            assert!(!success);
            assert_eq!(term, 5);
        }
        _ => panic!("Expected AppendEntriesResponse"),
    }
}

#[test]
fn candidate_steps_down_on_valid_append_entries() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.start_election();
    let resp = n.handle_append_entries_request("A".to_string(), 1);
    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => assert!(success),
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert!(is_follower(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
}

#[test]
fn leader_steps_down_on_higher_term_append_entries() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("A".to_string());
    let resp = n.handle_append_entries_request("B".to_string(), 5);
    match resp {
        RaftMessage::AppendEntriesResponse { success, term, .. } => {
            assert!(success);
            assert_eq!(term, 5);
        }
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert!(is_follower(&n));
    assert_eq!(n.leader_id, Some("B".to_string()));
}

#[test]
fn accept_append_entries_from_new_leader() {
    let mut n = node("C", vec![peer("A", "A", 0), peer("B", "B", 1)]);
    n.term = 1;
    n.leader_id = Some("A".to_string());
    let resp = n.handle_append_entries_request("B".to_string(), 2);
    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => assert!(success),
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert_eq!(n.leader_id, Some("B".to_string()));
    assert_eq!(n.term, 2);
}

#[test]
fn append_entries_clears_voted_for() {
    let mut n = node("B", vec![peer("A", "A", 0)]);
    n.voted_for = Some("C".to_string());
    n.term = 1;
    n.handle_append_entries_request("A".to_string(), 2);
    assert!(n.voted_for.is_none());
}

#[test]
fn append_entries_resets_election_timer() {
    let mut n = node("B", vec![peer("A", "A", 0)]);
    n.last_heartbeat = Instant::now() - Duration::from_millis(400);
    assert!(n.should_start_election());
    n.handle_append_entries_request("A".to_string(), 1);
    assert!(!n.should_start_election());
}

#[test]
fn repeated_heartbeats_keep_follower_alive() {
    let mut n = node("B", vec![peer("A", "A", 0)]);
    for _ in 1..=10 {
        n.handle_append_entries_request("A".to_string(), 1);
        assert!(is_follower(&n));
        assert!(!n.should_start_election());
    }
}

#[test]
fn leader_stays_leader_on_same_term_response() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("A".to_string());
    n.handle_append_entries_response("B".to_string(), 3, true);
    assert!(is_leader(&n));
}

#[test]
fn leader_stays_leader_on_lower_term_response() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.role = Role::Leader;
    n.term = 5;
    n.leader_id = Some("A".to_string());
    n.handle_append_entries_response("B".to_string(), 3, true);
    assert!(is_leader(&n));
}

#[test]
fn leader_steps_down_when_response_has_higher_term() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("A".to_string());
    n.handle_append_entries_response("B".to_string(), 5, true);
    assert!(is_follower(&n));
    assert_eq!(n.term, 5);
}

// ═══════════════════════════════════════════════════════════
// QUORUM
// ═══════════════════════════════════════════════════════════

#[test]
fn quorum_1_node() {
    assert_eq!(node("A", vec![]).quorum(), 1);
}

#[test]
fn quorum_2_nodes() {
    assert_eq!(node("A", vec![peer("B", "B", 1)]).quorum(), 2);
}

#[test]
fn quorum_3_nodes() {
    assert_eq!(
        node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]).quorum(),
        2
    );
}

#[test]
fn quorum_4_nodes() {
    assert_eq!(
        node(
            "A",
            vec![peer("B", "B", 1), peer("C", "C", 2), peer("D", "D", 3)]
        )
        .quorum(),
        3
    );
}

#[test]
fn quorum_5_nodes() {
    assert_eq!(
        node(
            "A",
            vec![
                peer("B", "B", 1),
                peer("C", "C", 2),
                peer("D", "D", 3),
                peer("E", "E", 4)
            ]
        )
        .quorum(),
        3
    );
}

// ═══════════════════════════════════════════════════════════
// ELECTION TIMEOUT
// ═══════════════════════════════════════════════════════════

#[test]
fn election_timeout_in_valid_range() {
    for _ in 0..100 {
        let n = node("A", vec![peer("B", "B", 1)]);
        assert!(n.election_timeout_ms >= 150);
        assert!(n.election_timeout_ms <= 300);
    }
}

#[test]
fn should_not_start_election_immediately() {
    let n = node("A", vec![peer("B", "B", 1)]);
    assert!(!n.should_start_election());
}

#[test]
fn should_start_election_after_timeout() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.last_heartbeat = Instant::now() - Duration::from_millis(500);
    assert!(n.should_start_election());
}

// ═══════════════════════════════════════════════════════════
// PEER MANAGEMENT
// ═══════════════════════════════════════════════════════════

#[test]
fn add_peer_increases_peer_count() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.add_peer(peer("C", "C", 2));
    assert_eq!(n.peers.len(), 2);
}

#[test]
fn add_peer_deduplicates_by_node_id() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.add_peer(peer("B", "B", 1));
    assert_eq!(n.peers.len(), 1);
}

#[test]
fn add_peer_rejects_self() {
    let mut n = node("A", vec![]);
    n.add_peer(peer("A", "A", 0));
    assert_eq!(n.peers.len(), 0);
}

#[test]
fn add_peer_updates_quorum() {
    let mut n = node("A", vec![]);
    assert_eq!(n.quorum(), 1);
    n.add_peer(peer("B", "B", 1));
    assert_eq!(n.quorum(), 2);
    n.add_peer(peer("C", "C", 2));
    assert_eq!(n.quorum(), 2);
}

#[test]
fn remove_peer_decreases_count() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.remove_peer("B");
    assert_eq!(n.peers.len(), 1);
    assert_eq!(n.peers[0].node_id, "C");
}

#[test]
fn remove_nonexistent_peer_is_noop() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.remove_peer("Z");
    assert_eq!(n.peers.len(), 1);
}

#[test]
fn is_peer_by_node_id_works() {
    let n = node("A", vec![peer("B", "B", 1)]);
    assert!(n.is_peer_by_node_id("B"));
    assert!(!n.is_peer_by_node_id("C"));
}

#[test]
fn is_peer_by_addr_works() {
    let n = node("A", vec![peer("B", "127.0.0.1", 7071)]);
    assert!(n.is_peer_by_addr("http://127.0.0.1:7071"));
    assert!(!n.is_peer_by_addr("http://127.0.0.1:9999"));
}

// ═══════════════════════════════════════════════════════════
// OFFLINE TIMEOUT / NODE FAILURE DETECTION
// ═══════════════════════════════════════════════════════════

#[test]
fn offline_timeout_removes_stale_peers() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    // Make B's last_seen very old
    n.peers[0].last_seen = Instant::now() - Duration::from_secs(30);

    let msgs = n.handle_offline_timeout(Instant::now(), 10_000);

    assert_eq!(n.peers.len(), 1);
    assert_eq!(n.peers[0].node_id, "C");
    // Should send RemovePeerRequest to C about B
    assert!(!msgs.is_empty());
}

#[test]
fn offline_timeout_keeps_fresh_peers() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    let msgs = n.handle_offline_timeout(Instant::now(), 10_000);
    assert_eq!(n.peers.len(), 2);
    assert!(msgs.is_empty());
}

#[test]
fn offline_timeout_removes_all_dead_peers() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.peers[0].last_seen = Instant::now() - Duration::from_secs(30);
    n.peers[1].last_seen = Instant::now() - Duration::from_secs(30);

    n.handle_offline_timeout(Instant::now(), 10_000);

    assert_eq!(n.peers.len(), 0);
}

#[test]
fn offline_timeout_notifies_remaining_peers() {
    let mut n = node(
        "A",
        vec![peer("B", "B", 1), peer("C", "C", 2), peer("D", "D", 3)],
    );
    // Kill B
    n.peers[0].last_seen = Instant::now() - Duration::from_secs(30);

    let msgs = n.handle_offline_timeout(Instant::now(), 10_000);

    // Should notify C and D to remove B
    assert_eq!(msgs.len(), 2);
    for msg in &msgs {
        if let RaftMessage::RemovePeerRequest { peer_node_id, .. } = msg {
            assert_eq!(peer_node_id, "B");
        } else {
            panic!("Expected RemovePeerRequest");
        }
    }
}

#[test]
fn offline_timeout_changes_quorum() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    assert_eq!(n.quorum(), 2); // 3 nodes, need 2

    n.peers[0].last_seen = Instant::now() - Duration::from_secs(30);
    n.handle_offline_timeout(Instant::now(), 10_000);

    assert_eq!(n.quorum(), 2); // 2 nodes, need 2
}

#[test]
fn update_last_seen_refreshes_peer() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    let old_time = Instant::now() - Duration::from_secs(60);
    n.peers[0].last_seen = old_time;

    // Receiving a vote request updates last_seen
    n.handle_vote_request("B".to_string(), 1);

    assert!(n.peers[0].last_seen > old_time);
}

// ═══════════════════════════════════════════════════════════
// FULL SCENARIOS: ELECTION
// ═══════════════════════════════════════════════════════════

#[test]
fn full_election_3_nodes() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);

    assert!(is_leader(&a));
    assert_eq!(a.leader_id, Some("A".to_string()));
    assert_eq!(b.voted_for, Some("A".to_string()));
    assert_eq!(c.voted_for, Some("A".to_string()));
}

#[test]
fn full_election_with_split_vote() {
    let (mut a, mut b, mut c) = three_nodes();
    let a_msgs = a.start_election();
    let b_msgs = b.start_election();

    // C votes for A first
    for msg in a_msgs {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
            && to == "http://C:2"
        {
            let resp = c.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                a.handle_vote_response("C".to_string(), term, granted);
            }
        }
    }
    // C rejects B
    for msg in b_msgs {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
            && to == "http://C:2"
        {
            let resp = c.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                b.handle_vote_response("C".to_string(), term, granted);
            }
        }
    }
    assert!(is_leader(&a));
    assert!(is_candidate(&b));
}

#[test]
fn two_node_cluster_elects_leader() {
    let mut a = node("A", vec![peer("B", "B", 1)]);
    let mut b = node("B", vec![peer("A", "A", 0)]);
    let msgs = a.start_election();
    for msg in msgs {
        if let RaftMessage::VoteRequest {
            candidate_id, term, ..
        } = msg
        {
            let resp = b.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                a.handle_vote_response("B".to_string(), term, granted);
            }
        }
    }
    assert!(is_leader(&a));
    assert_eq!(b.voted_for, Some("A".to_string()));
}

// ═══════════════════════════════════════════════════════════
// FULL SCENARIOS: NODE FAILURE & RECOVERY
// ═══════════════════════════════════════════════════════════

#[test]
fn leader_dies_new_leader_elected() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);
    assert!(is_leader(&a));

    // A "dies" — B and C stop receiving heartbeats
    // B starts election
    let msgs = b.start_election();
    assert_eq!(b.term, 2);

    // B asks C for vote (A is dead, won't respond)
    for msg in msgs {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
            && to == "http://C:2"
        {
            let resp = c.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                b.handle_vote_response("C".to_string(), term, granted);
            }
        }
    }

    // B wins with votes from B + C (2 of 3)
    assert!(is_leader(&b));
    assert_eq!(b.leader_id, Some("B".to_string()));
    assert_eq!(b.term, 2);
}

#[test]
fn dead_node_rejoins_as_follower() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);
    assert!(is_leader(&a));

    // A "dies", B becomes leader
    let msgs = b.start_election();
    for msg in msgs {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
            && to == "http://C:2"
        {
            let resp = c.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                b.handle_vote_response("C".to_string(), term, granted);
            }
        }
    }
    assert!(is_leader(&b));

    // A "comes back" — receives AppendEntries from B
    // A is still at term 1, B is at term 2
    let resp = a.handle_append_entries_request("B".to_string(), b.term);

    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => assert!(success),
        _ => panic!("Expected AppendEntriesResponse"),
    }

    assert!(is_follower(&a));
    assert_eq!(a.leader_id, Some("B".to_string()));
    assert_eq!(a.term, 2);
}

#[test]
fn old_leader_steps_down_on_higher_term() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);
    assert!(is_leader(&a));
    assert_eq!(a.term, 1);

    // A gets a vote request from B at term 5 (B had many failed elections)
    let resp = a.handle_vote_request("B".to_string(), 5);

    match resp {
        RaftMessage::VoteResponse { granted, .. } => assert!(granted),
        _ => panic!("Expected VoteResponse"),
    }
    assert!(is_follower(&a));
    assert_eq!(a.term, 5);
}

#[test]
fn five_node_cluster_tolerates_two_failures() {
    let (mut a, mut b, mut c, mut _d, mut _e) = five_nodes();

    // A wins election with votes from B and C (3 of 5)
    let msgs = a.start_election();
    for msg in msgs {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
        {
            let (voter_id, voter) = match to.as_str() {
                "http://B:1" => ("B".to_string(), &mut b),
                "http://C:2" => ("C".to_string(), &mut c),
                _ => continue, // D and E are "dead"
            };
            let resp = voter.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                a.handle_vote_response(voter_id, term, granted);
            }
            if is_leader(&a) {
                break;
            }
        }
    }
    assert!(is_leader(&a));
    // A has 3 votes: self + B + C — that's quorum for 5 nodes
}

#[test]
fn leader_change_via_append_entries_sequence() {
    let mut n = node("C", vec![peer("A", "A", 0), peer("B", "B", 1)]);

    // A is leader at term 1
    n.handle_append_entries_request("A".to_string(), 1);
    assert_eq!(n.leader_id, Some("A".to_string()));

    // B becomes leader at term 2
    n.handle_append_entries_request("B".to_string(), 2);
    assert_eq!(n.leader_id, Some("B".to_string()));
    assert_eq!(n.term, 2);

    // A sends stale heartbeat from term 1 — rejected
    let resp = n.handle_append_entries_request("A".to_string(), 1);
    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => assert!(!success),
        _ => panic!("Expected AppendEntriesResponse"),
    }
    // Leader is still B
    assert_eq!(n.leader_id, Some("B".to_string()));
}

#[test]
fn full_heartbeat_cycle() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);
    assert!(is_leader(&a));

    send_heartbeats_3(&mut a, &mut b, &mut c);

    assert!(is_leader(&a));
    assert!(is_follower(&b));
    assert!(is_follower(&c));
    assert_eq!(b.leader_id, Some("A".to_string()));
    assert_eq!(c.leader_id, Some("A".to_string()));
    assert_eq!(a.term, b.term);
    assert_eq!(a.term, c.term);
}

#[test]
fn multiple_heartbeat_rounds_maintain_stability() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);

    for _ in 0..10 {
        send_heartbeats_3(&mut a, &mut b, &mut c);
        assert!(is_leader(&a));
        assert!(is_follower(&b));
        assert!(is_follower(&c));
    }
}

// ═══════════════════════════════════════════════════════════
// FULL SCENARIOS: PEER MEMBERSHIP CHANGES
// ═══════════════════════════════════════════════════════════

#[test]
fn node_joins_existing_cluster() {
    let mut a = node("A", vec![]);
    a.start_election(); // A is leader alone
    assert!(is_leader(&a));

    // B joins
    a.add_peer(peer("B", "B", 1));
    // Leader should step down because quorum changed
    assert_eq!(a.peers.len(), 1);
    assert_eq!(a.quorum(), 2);
}

#[test]
fn removing_peer_changes_quorum() {
    let mut n = node(
        "A",
        vec![peer("B", "B", 1), peer("C", "C", 2), peer("D", "D", 3)],
    );
    assert_eq!(n.quorum(), 3); // 4 nodes

    n.remove_peer("D");
    assert_eq!(n.quorum(), 2); // 3 nodes

    n.remove_peer("C");
    assert_eq!(n.quorum(), 2); // 2 nodes
}

#[test]
fn handle_add_peer_request_returns_success() {
    let mut n = node("A", vec![]);
    let resp = n.handle_add_peer_request(peer("B", "B", 1));
    match resp {
        RaftMessage::AddPeerResponse { success } => assert!(success),
        _ => panic!("Expected AddPeerResponse"),
    }
    assert_eq!(n.peers.len(), 1);
}

#[test]
fn handle_remove_peer_request_returns_success() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    let resp = n.handle_remove_peer_request("B".to_string());
    match resp {
        RaftMessage::RemovePeerResponse { success } => assert!(success),
        _ => panic!("Expected RemovePeerResponse"),
    }
    assert_eq!(n.peers.len(), 0);
}

// ═══════════════════════════════════════════════════════════
// FULL SCENARIOS: CASCADING FAILURES
// ═══════════════════════════════════════════════════════════

#[test]
fn leader_dies_then_new_leader_dies() {
    let (mut a, mut b, mut c) = three_nodes();

    // Round 1: A becomes leader
    elect_leader_3(&mut a, &mut b, &mut c);
    assert!(is_leader(&a));
    assert_eq!(a.term, 1);

    // Round 2: A dies, B becomes leader
    let msgs = b.start_election();
    for msg in msgs {
        if let RaftMessage::VoteRequest {
            to,
            candidate_id,
            term,
        } = msg
            && to == "http://C:2"
        {
            let resp = c.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                b.handle_vote_response("C".to_string(), term, granted);
            }
        }
    }
    assert!(is_leader(&b));
    assert_eq!(b.term, 2);

    // Round 3: B dies, C becomes leader (only node left that's alive)
    // C removes A and B from peers (offline timeout)
    c.peers.retain(|p| p.node_id == "C"); // simulate cleanup
    c.peers.clear(); // no peers left

    let msgs = c.start_election();
    assert!(is_leader(&c)); // quorum of 1
    assert!(msgs.is_empty());
    assert_eq!(c.term, 3);
}

#[test]
fn stale_leader_reconnects_to_higher_term_cluster() {
    let (mut a, mut b, mut c) = three_nodes();
    elect_leader_3(&mut a, &mut b, &mut c);
    assert!(is_leader(&a));

    // Network partition: A is isolated
    // B and C have 10 election rounds
    for _ in 0..10 {
        b.start_election();
    }
    // B is at term 11, A is at term 1

    // Network heals: A receives AppendEntries from B
    let resp = a.handle_append_entries_request("B".to_string(), b.term);
    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => assert!(success),
        _ => panic!("Expected AppendEntriesResponse"),
    }

    assert!(is_follower(&a));
    assert_eq!(a.term, b.term);
    assert_eq!(a.leader_id, Some("B".to_string()));
}

// ═══════════════════════════════════════════════════════════
// EDGE CASES
// ═══════════════════════════════════════════════════════════

#[test]
fn vote_request_at_exact_same_term() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.term = 5;
    // Request at same term, haven't voted yet
    let resp = n.handle_vote_request("A".to_string(), 5);
    match resp {
        RaftMessage::VoteResponse { granted, .. } => assert!(granted),
        _ => panic!("Expected VoteResponse"),
    }
}

#[test]
fn candidate_receiving_append_entries_at_same_term_steps_down() {
    // B is candidate at term 3, receives AppendEntries from A at term 3
    // This means A won the election — B must accept
    let mut b = node("B", vec![peer("A", "A", 0)]);
    b.role = Role::Candidate;
    b.term = 3;
    b.voted_for = Some("B".to_string());

    let resp = b.handle_append_entries_request("A".to_string(), 3);
    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => assert!(success),
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert!(is_follower(&b));
    assert_eq!(b.leader_id, Some("A".to_string()));
}

#[test]
fn follower_does_not_step_down_on_higher_term_vote_response() {
    // A follower shouldn't change state from a vote response
    // it never solicited
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.term = 1;
    let msgs = n.handle_vote_response("B".to_string(), 5, true);
    assert!(is_follower(&n));
    // Term should NOT be updated because we're not a candidate
    assert_eq!(n.term, 1);
    assert!(msgs.is_empty());
}

#[test]
fn leader_ignores_vote_response_from_higher_term() {
    // Leader at term 3 gets a vote response with term 5
    // Leader is not a candidate, should ignore
    let mut n = node("A", vec![peer("B", "B", 1)]);
    n.role = Role::Leader;
    n.term = 3;
    let msgs = n.handle_vote_response("B".to_string(), 5, false);
    assert!(is_leader(&n)); // should still be leader
    assert!(msgs.is_empty());
}

#[test]
fn rapid_election_cycles_converge() {
    // Simulate 100 election cycles — term should match cycle count
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    for i in 1..=100 {
        n.start_election();
        assert_eq!(n.term, i);
        assert_eq!(n.votes_received.len(), 1);
        assert!(n.votes_received.contains("A"));
    }
}

#[test]
fn peer_addr_format() {
    let p = peer("B", "192.168.1.100", 7071);
    assert_eq!(p.addr(), "http://192.168.1.100:7071");
}
