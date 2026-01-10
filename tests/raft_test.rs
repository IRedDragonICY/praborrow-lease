use async_trait::async_trait;
use praborrow_lease::network::{ConsensusNetwork, Packet};
use praborrow_lease::raft::{InMemoryStorage, RaftNode, RaftRole, RaftStorage};

/// Mock network for testing.
struct MockNetwork;

#[async_trait]
impl ConsensusNetwork for MockNetwork {
    async fn broadcast_vote_request(&self, _term: u64, _candidate_id: u128) -> Result<(), String> {
        Ok(())
    }

    async fn send_heartbeat(&self, _leader_id: u128, _term: u64) -> Result<(), String> {
        Ok(())
    }

    async fn receive(&self) -> Result<Packet, String> {
        Err("Mock network - no packets".to_string())
    }

    async fn update_peers(&self, _peers: Vec<String>) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn test_leader_election_start() {
    let storage: Box<dyn RaftStorage<String>> = Box::new(InMemoryStorage::new());
    let mut node = RaftNode::<String>::new(1, Box::new(MockNetwork), storage);

    assert_eq!(node.storage.get_term().unwrap(), 0);
    assert_eq!(node.role, RaftRole::Follower);

    // Start election
    node.start_election().await;

    assert_eq!(node.storage.get_term().unwrap(), 1);
    assert_eq!(node.role, RaftRole::Candidate);
    assert_eq!(node.storage.get_vote().unwrap(), Some(1));
}

#[test]
fn test_vote_handling() {
    let storage: Box<dyn RaftStorage<String>> = Box::new(InMemoryStorage::new());
    let mut node = RaftNode::<String>::new(1, Box::new(MockNetwork), storage);

    // Receive vote request from 2 for term 1
    let granted = node.handle_request_vote(1, 2);

    assert!(granted);
    assert_eq!(node.storage.get_vote().unwrap(), Some(2));
    assert_eq!(node.storage.get_term().unwrap(), 1);

    // Deny vote for same term from 3
    let granted_again = node.handle_request_vote(1, 3);
    assert!(!granted_again);
}

#[test]
fn test_raft_node_with_memory_storage() {
    // Test the convenience constructor
    let node = RaftNode::<String>::with_memory_storage(42, Box::new(MockNetwork));

    assert_eq!(node.id, 42);
    assert_eq!(node.role, RaftRole::Follower);
    assert_eq!(node.storage.get_term().unwrap(), 0);
}
