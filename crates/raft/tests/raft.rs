
use std::collections::HashSet;

use raft::{RaftMessage, RaftNode, Role};

// ── Helpers ──────────────────────────────────────────────

fn node(id: &str, peers: Vec<&str>) -> RaftNode {
    RaftNode::new(
        id.to_string(),
        peers.into_iter().map(String::from).collect(),
    )
}

fn is_follower(n: &RaftNode) -> bool { matches!(n.role, Role::Follower) }
fn is_candidate(n: &RaftNode) -> bool { matches!(n.role, Role::Candidate) }
fn is_leader(n: &RaftNode) -> bool { matches!(n.role, Role::Leader) }

// ── Election basics ─────────────────────────────────────

#[test]
fn new_node_starts_as_follower() {
    let n = node("A", vec!["B", "C"]);
    assert!(is_follower(&n));
    assert_eq!(n.term, 0);
    assert!(n.voted_for.is_none());
    assert!(n.leader_id.is_none());
}

#[test]
fn start_election_transitions_to_candidate() {
    let mut n = node("A", vec!["B", "C"]);
    let msgs = n.start_election();

    assert!(is_candidate(&n));
    assert_eq!(n.term, 1);
    assert_eq!(n.voted_for, Some("A".to_string()));
    assert!(n.votes_received.contains("A"));
    assert_eq!(msgs.len(), 2); // one VoteRequest per peer
}

#[test]
fn start_election_sends_correct_vote_requests() {
    let mut n = node("A", vec!["B", "C"]);
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
    assert!(targets.contains("B"));
    assert!(targets.contains("C"));
}

// ── Vote granting ───────────────────────────────────────

#[test]
fn follower_grants_vote_to_first_candidate() {
    let mut n = node("B", vec!["A", "C"]);
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
    let mut n = node("C", vec!["A", "B"]);
    // Vote for A in term 1
    n.handle_vote_request("A".to_string(), 1);

    // B also asks for vote in term 1
    let resp = n.handle_vote_request("B".to_string(), 1);

    match resp {
        RaftMessage::VoteResponse { granted, .. } => {
            assert!(!granted);
        }
        _ => panic!("Expected VoteResponse"),
    }
    // Still voted for A, not B
    assert_eq!(n.voted_for, Some("A".to_string()));
}

#[test]
fn follower_grants_vote_again_to_same_candidate() {
    let mut n = node("B", vec!["A", "C"]);
    n.handle_vote_request("A".to_string(), 1);

    // Same candidate asks again (e.g. retransmit)
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
    let mut n = node("B", vec!["A", "C"]);
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
    let mut n = node("B", vec!["A", "C"]);
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
    let mut n = node("A", vec!["B", "C"]);
    n.start_election(); // term 1, votes: {A}

    // B votes yes
    let msgs = n.handle_vote_response("B".to_string(), 1, true);

    // A has 2 out of 3 — that's a majority
    assert!(is_leader(&n));
    assert_eq!(n.leader_id, Some("A".to_string()));
    // Should send AppendEntries to all peers
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
    let mut n = node("A", vec!["B", "C", "D", "E"]);
    n.start_election(); // 5 nodes, quorum = 3, votes: {A}

    // Only B votes yes — that's 2/5, not enough
    let msgs = n.handle_vote_response("B".to_string(), 1, true);

    assert!(is_candidate(&n));
    assert!(msgs.is_empty());
}

#[test]
fn candidate_steps_down_on_higher_term_in_vote_response() {
    let mut n = node("A", vec!["B", "C"]);
    n.start_election();

    // B responds with a higher term (B has seen term 5 somehow)
    let msgs = n.handle_vote_response("B".to_string(), 5, false);

    assert!(is_follower(&n));
    assert_eq!(n.term, 5);
    assert!(n.voted_for.is_none());
    assert!(n.votes_received.is_empty());
    assert!(msgs.is_empty());
}

#[test]
fn duplicate_vote_from_same_node_does_not_double_count() {
    let mut n = node("A", vec!["B", "C", "D", "E"]);
    n.start_election(); // 5 nodes, quorum = 3, votes: {A}

    n.handle_vote_response("B".to_string(), 1, true); // votes: {A, B}
    let msgs = n.handle_vote_response("B".to_string(), 1, true); // still {A, B}

    // Should NOT be leader — only 2 unique votes, need 3
    assert!(is_candidate(&n));
    assert!(msgs.is_empty());
}

// ── AppendEntries (heartbeat) ───────────────────────────

#[test]
fn follower_accepts_append_entries_from_leader() {
    let mut n = node("B", vec!["A", "C"]);

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
    let mut n = node("B", vec!["A", "C"]);
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
    let mut n = node("B", vec!["A", "C"]);
    n.start_election(); // B is now candidate in term 1

    // A also won an election in term 1 (or higher)
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
    let mut n = node("A", vec!["B", "C"]);
    n.role = Role::Leader;
    n.term = 3;
    n.leader_id = Some("A".to_string());

    // B became leader in term 5
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
    // This tests the bug: a node whose leader_id is "A"
    // must still accept AppendEntries from "B" if B's term is valid
    let mut n = node("C", vec!["A", "B"]);
    n.term = 1;
    n.leader_id = Some("A".to_string());

    // New election happened, B is now leader in term 2
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
    let n = node("A", vec!["B", "C"]);
    assert_eq!(n.quorum(), 2); // majority of 3 = 2
}

#[test]
fn quorum_5_nodes() {
    let n = node("A", vec!["B", "C", "D", "E"]);
    assert_eq!(n.quorum(), 3); // majority of 5 = 3
}

#[test]
fn quorum_1_node() {
    let n = node("A", vec![]);
    assert_eq!(n.quorum(), 1); // single node cluster, always has majority
}

// ── Election timeout ────────────────────────────────────

#[test]
fn election_timeout_in_valid_range() {
    for _ in 0..100 {
        let n = node("A", vec!["B"]);
        assert!(n.election_timeout_ms >= 150);
        assert!(n.election_timeout_ms <= 300);
    }
}

#[test]
fn should_not_start_election_immediately() {
    let mut n = node("A", vec!["B"]);
    assert!(!n.should_start_election());
}

// ── Full scenario: 3-node election ──────────────────────

#[test]
fn full_election_scenario() {
    let mut a = node("A", vec!["B", "C"]);
    let mut b = node("B", vec!["A", "C"]);
    let mut c = node("C", vec!["A", "B"]);

    // A starts election
    let vote_requests = a.start_election();
    assert!(is_candidate(&a));
    assert_eq!(a.term, 1);

    // Deliver vote requests to B and C
    for msg in vote_requests {
        match msg {
            RaftMessage::VoteRequest { to, candidate_id, term } => {
                let response = if to == "B" {
                    b.handle_vote_request(candidate_id, term)
                } else {
                    c.handle_vote_request(candidate_id, term)
                };

                // Deliver response back to A
                match response {
                    RaftMessage::VoteResponse { term, granted, .. } => {
                        a.handle_vote_response(
                            to.clone(),
                            term,
                            granted,
                        );
                    }
                    _ => panic!("Expected VoteResponse"),
                }
            }
            _ => panic!("Expected VoteRequest"),
        }
    }

    // A should be leader now
    assert!(is_leader(&a));
    assert_eq!(a.leader_id, Some("A".to_string()));

    // B and C voted for A
    assert_eq!(b.voted_for, Some("A".to_string()));
    assert_eq!(c.voted_for, Some("A".to_string()));
}
