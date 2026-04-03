use std::collections::HashSet;
use std::time::Duration;

use raft::{Peer, RaftMessage, RaftNode, Role};
use std::time::Instant;

// ── Helpers ──────────────────────────────────────────────

fn peer(id: &str, ip: &str, port: u32) -> Peer {
    Peer {
        node_id: id.to_string(),
        hostname: id.to_string(),
        ip_address: ip.to_string(),
        port,
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

fn is_follower(n: &RaftNode) -> bool { matches!(n.role, Role::Follower) }
fn is_candidate(n: &RaftNode) -> bool { matches!(n.role, Role::Candidate) }
fn is_leader(n: &RaftNode) -> bool { matches!(n.role, Role::Leader) }

// ── Election basics ─────────────────────────────────────

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
            RaftMessage::VoteRequest { term, candidate_id, .. } => {
                assert_eq!(*term, 1);
                assert_eq!(candidate_id, "A");
            }
            _ => panic!("Expected VoteRequest"),
        }
    }

    let targets: HashSet<String> = msgs.iter().map(|m| match m {
        RaftMessage::VoteRequest { to, .. } => to.clone(),
        _ => unreachable!(),
    }).collect();
    assert!(targets.contains("http://B:1"));
    assert!(targets.contains("http://C:2"));
}

// ── Vote granting ───────────────────────────────────────

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
        RaftMessage::VoteResponse { granted, .. } => {
            assert!(!granted);
        }
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
        RaftMessage::VoteResponse { granted, .. } => {
            assert!(granted);
        }
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

// ── Winning an election ─────────────────────────────────

#[test]
fn candidate_becomes_leader_with_majority() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();

    let msgs = n.handle_vote_response("B".to_string(), 1, true);

    assert!(is_leader(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
    assert_eq!(msgs.len(), 2);
    for msg in &msgs {
        match msg {
            RaftMessage::AppendEntriesRequest { term, leader_id, .. } => {
                assert_eq!(*term, 1);
                assert_eq!(leader_id, "A");
            }
            _ => panic!("Expected AppendEntriesRequest"),
        }
    }
}

#[test]
fn candidate_does_not_become_leader_without_majority() {
    let mut n = node("A", vec![
        peer("B", "B", 1), peer("C", "C", 2),
        peer("D", "D", 3), peer("E", "E", 4),
    ]);
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
    let mut n = node("A", vec![
        peer("B", "B", 1), peer("C", "C", 2),
        peer("D", "D", 3), peer("E", "E", 4),
    ]);
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

// ── AppendEntries (heartbeat) ───────────────────────────

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
    assert_eq!(n.term, 1);
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
        RaftMessage::AppendEntriesResponse { success, .. } => {
            assert!(success);
        }
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
    assert_eq!(n.term, 5);
}

#[test]
fn accept_append_entries_from_new_leader() {
    let mut n = node("C", vec![peer("A", "A", 0), peer("B", "B", 1)]);
    n.term = 1;
    n.leader_id = Some("A".to_string());

    let resp = n.handle_append_entries_request("B".to_string(), 2);

    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => {
            assert!(success);
        }
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert_eq!(n.leader_id, Some("B".to_string()));
    assert_eq!(n.term, 2);
}

// ── Quorum calculation ──────────────────────────────────

#[test]
fn quorum_3_nodes() {
    let n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    assert_eq!(n.quorum(), 2);
}

#[test]
fn quorum_5_nodes() {
    let n = node("A", vec![
        peer("B", "B", 1), peer("C", "C", 2),
        peer("D", "D", 3), peer("E", "E", 4),
    ]);
    assert_eq!(n.quorum(), 3);
}

#[test]
fn quorum_1_node() {
    let n = node("A", vec![]);
    assert_eq!(n.quorum(), 1);
}

// ── Election timeout ────────────────────────────────────

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

// ── Full scenario: 3-node election ──────────────────────

#[test]
fn full_election_scenario() {
    let (mut a, mut b, mut c) = three_nodes();

    let vote_requests = a.start_election();
    assert!(is_candidate(&a));
    assert_eq!(a.term, 1);

    for msg in vote_requests {
        if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
            let (voter_id, response) = if to == "http://B:1" {
                ("B".to_string(), b.handle_vote_request(candidate_id, term))
            } else {
                ("C".to_string(), c.handle_vote_request(candidate_id, term))
            };

            if let RaftMessage::VoteResponse { term, granted, .. } = response {
                a.handle_vote_response(voter_id, term, granted);
            }
        }
    }

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

    for msg in a_msgs {
        if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
            if to == "http://C:2" {
                let resp = c.handle_vote_request(candidate_id, term);
                if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                    a.handle_vote_response("C".to_string(), term, granted);
                }
            }
        }
    }

    for msg in b_msgs {
        if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
            if to == "http://C:2" {
                let resp = c.handle_vote_request(candidate_id, term);
                if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                    b.handle_vote_response("C".to_string(), term, granted);
                }
            }
        }
    }

    assert!(is_leader(&a));
    assert!(is_candidate(&b));
}

// ── AppendEntries additional tests ──────────────────────

#[test]
fn leader_sends_heartbeats_after_winning() {
    let (mut a, mut b, mut c) = three_nodes();

    let vote_requests = a.start_election();

    for msg in vote_requests {
        if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
            let (voter_id, resp) = if to == "http://B:1" {
                ("B".to_string(), b.handle_vote_request(candidate_id, term))
            } else {
                ("C".to_string(), c.handle_vote_request(candidate_id, term))
            };

            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                let follow_up = a.handle_vote_response(voter_id, term, granted);

                if is_leader(&a) {
                    assert_eq!(follow_up.len(), 2);
                    for msg in &follow_up {
                        assert!(matches!(msg, RaftMessage::AppendEntriesRequest { .. }));
                    }
                    break;
                }
            }
        }
    }

    assert!(is_leader(&a));
}

#[test]
fn follower_recognizes_new_leader_via_append_entries() {
    let (_, mut b, _) = three_nodes();

    assert!(b.leader_id.is_none());

    b.handle_append_entries_request("A".to_string(), 1);

    assert_eq!(b.leader_id, Some("A".to_string()));
    assert!(is_follower(&b));
}

#[test]
fn append_entries_clears_voted_for() {
    let mut n = node("B", vec![peer("A", "A", 0), peer("C", "C", 2)]);
    n.voted_for = Some("C".to_string());
    n.term = 1;

    n.handle_append_entries_request("A".to_string(), 2);

    assert!(n.voted_for.is_none());
}

#[test]
fn append_entries_from_same_term_accepted() {
    let mut n = node("B", vec![peer("A", "A", 0)]);
    n.term = 3;

    let resp = n.handle_append_entries_request("A".to_string(), 3);

    match resp {
        RaftMessage::AppendEntriesResponse { success, term, .. } => {
            assert!(success);
            assert_eq!(term, 3);
        }
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert_eq!(n.leader_id, Some("A".to_string()));
}

#[test]
fn repeated_heartbeats_keep_follower_alive() {
    let mut n = node("B", vec![peer("A", "A", 0)]);

    for _ in 1..=5 {
        n.handle_append_entries_request("A".to_string(), 1);
        assert!(is_follower(&n));
        assert!(!n.should_start_election());
        assert_eq!(n.leader_id, Some("A".to_string()));
    }
}

#[test]
fn leader_steps_down_when_response_has_higher_term() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("A".to_string());

    n.handle_append_entries_response(5, true);

    assert!(is_follower(&n));
    assert_eq!(n.term, 5);
    assert!(n.voted_for.is_none());
}

#[test]
fn leader_stays_leader_on_same_term_response() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("A".to_string());

    n.handle_append_entries_response(3, true);

    assert!(is_leader(&n));
    assert_eq!(n.term, 3);
}

#[test]
fn leader_stays_leader_on_lower_term_response() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.role = Role::Leader;
    n.term = 5;
    n.leader_id = Some("A".to_string());

    n.handle_append_entries_response(3, true);

    assert!(is_leader(&n));
    assert_eq!(n.term, 5);
}

#[test]
fn leader_change_via_append_entries() {
    let mut n = node("C", vec![peer("A", "A", 0), peer("B", "B", 1)]);

    n.handle_append_entries_request("A".to_string(), 1);
    assert_eq!(n.leader_id, Some("A".to_string()));

    n.handle_append_entries_request("B".to_string(), 2);
    assert_eq!(n.leader_id, Some("B".to_string()));
    assert_eq!(n.term, 2);

    let resp = n.handle_append_entries_request("A".to_string(), 1);
    match resp {
        RaftMessage::AppendEntriesResponse { success, .. } => {
            assert!(!success);
        }
        _ => panic!("Expected AppendEntriesResponse"),
    }
    assert_eq!(n.leader_id, Some("B".to_string()));
}

#[test]
fn full_heartbeat_cycle() {
    let (mut a, mut b, mut c) = three_nodes();

    let vote_requests = a.start_election();
    for msg in vote_requests {
        if let RaftMessage::VoteRequest { to, candidate_id, term } = msg {
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
    assert!(is_leader(&a));

    let heartbeats: Vec<_> = a.peers.iter().map(|p| RaftMessage::AppendEntriesRequest {
        to: p.addr(),
        term: a.term,
        leader_id: a.node_id.clone(),
    }).collect();

    for msg in heartbeats {
        if let RaftMessage::AppendEntriesRequest { to, term, leader_id } = msg {
            let resp = if to == "http://B:1" {
                b.handle_append_entries_request(leader_id, term)
            } else {
                c.handle_append_entries_request(leader_id, term)
            };

            if let RaftMessage::AppendEntriesResponse { term, success, .. } = resp {
                a.handle_append_entries_response(term, success);
            }
        }
    }

    assert!(is_leader(&a));
    assert!(is_follower(&b));
    assert!(is_follower(&c));
    assert_eq!(b.leader_id, Some("A".to_string()));
    assert_eq!(c.leader_id, Some("A".to_string()));
    assert_eq!(a.term, b.term);
    assert_eq!(a.term, c.term);
}

// ── Edge cases ──────────────────────────────────────────

#[test]
fn election_clears_previous_votes() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.start_election();
    n.start_election();

    assert_eq!(n.term, 2);
    assert_eq!(n.votes_received.len(), 1);
    assert!(n.votes_received.contains("A"));
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

#[test]
fn two_node_cluster_elects_leader() {
    let mut a = node("A", vec![peer("B", "B", 1)]);
    let mut b = node("B", vec![peer("A", "A", 0)]);

    let msgs = a.start_election();
    assert_eq!(msgs.len(), 1);

    for msg in msgs {
        if let RaftMessage::VoteRequest { candidate_id, term, .. } = msg {
            let resp = b.handle_vote_request(candidate_id, term);
            if let RaftMessage::VoteResponse { term, granted, .. } = resp {
                a.handle_vote_response("B".to_string(), term, granted);
            }
        }
    }

    assert!(is_leader(&a));
    assert_eq!(b.voted_for, Some("A".to_string()));
}

#[test]
fn leader_steps_down_on_higher_term_vote_response() {
    let mut n = node("A", vec![peer("B", "B", 1), peer("C", "C", 2)]);
    n.role = Role::Leader;
    n.term = 3;

    let msgs = n.handle_vote_response("B".to_string(), 5, false);

    assert!(is_leader(&n));
    assert!(msgs.is_empty());
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

#[test]
fn follower_updates_term_on_higher_term_append_entries() {
    let mut n = node("B", vec![peer("A", "A", 0)]);
    assert_eq!(n.term, 0);

    n.handle_append_entries_request("A".to_string(), 5);

    assert_eq!(n.term, 5);
    assert!(is_follower(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
}

// ── Add peer ────────────────────────────────────────────

#[test]
fn add_peer_increases_peer_count() {
    let mut n = node("A", vec![peer("B", "B", 1)]);
    assert_eq!(n.peers.len(), 1);

    n.handle_add_peer_request(peer("C", "C", 2));

    assert_eq!(n.peers.len(), 2);
    assert_eq!(n.peers[1].node_id, "C");
}

#[test]
fn add_peer_returns_success() {
    let mut n = node("A", vec![]);

    let resp = n.handle_add_peer_request(peer("B", "B", 1));

    match resp {
        RaftMessage::AddPeerResponse { success } => {
            assert!(success);
        }
        _ => panic!("Expected AddPeerResponse"),
    }
}

#[test]
fn add_peer_updates_quorum() {
    let mut n = node("A", vec![]);
    assert_eq!(n.quorum(), 1);

    n.handle_add_peer_request(peer("B", "B", 1));
    assert_eq!(n.quorum(), 2);

    n.handle_add_peer_request(peer("C", "C", 2));
    assert_eq!(n.quorum(), 2);
}
