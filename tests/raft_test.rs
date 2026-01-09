use praborrow_lease::raft::{RaftNode, RaftRole};

#[test]
fn test_leader_election_start() {
    let mut node = RaftNode::<String>::new(1);
    
    assert_eq!(node.current_term, 0);
    assert_eq!(node.role, RaftRole::Follower);

    // Start election
    node.start_election();

    assert_eq!(node.current_term, 1);
    assert_eq!(node.role, RaftRole::Candidate);
    assert_eq!(node.voted_for, Some(1));
}

#[test]
fn test_vote_handling() {
    let mut node = RaftNode::<String>::new(1);

    // Receive vote request from 2 for term 1
    let granted = node.handle_request_vote(1, 2);
    
    assert!(granted);
    assert_eq!(node.voted_for, Some(2));
    assert_eq!(node.current_term, 1);
    
    // Deny vote for same term from 3
    let granted_again = node.handle_request_vote(1, 3);
    assert!(!granted_again);
}
