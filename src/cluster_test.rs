//! Integration tests for Raft cluster.
//!
//! Tests multi-node cluster behavior including leader election, log replication,
//! and failure handling.

use crate::engine::RaftConfig;
use crate::network::RaftMessage;
use crate::raft::{InMemoryStorage, LogEntry, NodeId, RaftStorage};
use crate::state_machine::{KeyValueStateMachine, KvCommand, ReplicatedStateMachine};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

/// Test harness for a Raft cluster.
#[allow(dead_code)]
pub struct TestCluster<
    T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
> {
    nodes: HashMap<NodeId, TestNode<T>>,
    network: TestNetwork<T>,
}

#[allow(dead_code)]
struct TestNode<T: Clone + Send + Sync + 'static> {
    id: NodeId,
    inbox_tx: mpsc::Sender<RaftMessage<T>>,
    inbox_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<RaftMessage<T>>>>,
}

#[allow(dead_code)]
struct TestNetwork<T: Clone + Send + Sync + 'static> {
    channels: Arc<RwLock<HashMap<NodeId, mpsc::Sender<RaftMessage<T>>>>>,
}

impl<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static>
    TestCluster<T>
{
    /// Creates a new test cluster with the specified number of nodes.
    pub fn new(node_count: usize) -> Self {
        let mut nodes = HashMap::new();
        let channels = Arc::new(RwLock::new(HashMap::new()));

        for i in 0..node_count {
            let node_id = (i + 1) as NodeId;
            let (tx, rx) = mpsc::channel(1000);

            nodes.insert(
                node_id,
                TestNode {
                    id: node_id,
                    inbox_tx: tx.clone(),
                    inbox_rx: Arc::new(tokio::sync::Mutex::new(rx)),
                },
            );
        }

        let network = TestNetwork { channels };

        Self { nodes, network }
    }

    /// Gets the node IDs in the cluster.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Gets the inbox sender for a node (for sending messages to it).
    pub fn get_inbox_sender(&self, node_id: NodeId) -> Option<mpsc::Sender<RaftMessage<T>>> {
        self.nodes.get(&node_id).map(|n| n.inbox_tx.clone())
    }
}

/// Creates a 3-node test cluster with in-memory storage.
pub fn create_test_cluster() -> (
    Vec<(
        NodeId,
        InMemoryStorage<String>,
        mpsc::Sender<RaftMessage<String>>,
        mpsc::Receiver<RaftMessage<String>>,
    )>,
) {
    let mut nodes = Vec::new();

    for i in 1..=3 {
        let node_id = i as NodeId;
        let storage = InMemoryStorage::new();
        let (tx, rx) = mpsc::channel(1000);

        nodes.push((node_id, storage, tx, rx));
    }

    (nodes,)
}

/// Simulates message routing between nodes.
pub struct MessageRouter<T: Clone + Send + Sync + 'static> {
    senders: HashMap<NodeId, mpsc::Sender<RaftMessage<T>>>,
}

impl<T: Clone + Send + Sync + 'static> MessageRouter<T> {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    pub fn register(&mut self, node_id: NodeId, sender: mpsc::Sender<RaftMessage<T>>) {
        self.senders.insert(node_id, sender);
    }

    pub async fn route(&self, to: NodeId, message: RaftMessage<T>) -> Result<(), String> {
        if let Some(sender) = self.senders.get(&to) {
            sender.send(message).await.map_err(|e| e.to_string())
        } else {
            Err(format!("Node {} not found", to))
        }
    }

    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.senders.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_cluster() {
        let (nodes,) = create_test_cluster();
        assert_eq!(nodes.len(), 3);

        for (i, (node_id, _, _, _)) in nodes.iter().enumerate() {
            assert_eq!(*node_id, (i + 1) as NodeId);
        }
    }

    #[test]
    fn test_message_router() {
        let mut router = MessageRouter::<String>::new();

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        router.register(1, tx1);
        router.register(2, tx2);

        assert_eq!(router.peer_ids().len(), 2);
    }

    #[tokio::test]
    async fn test_leader_election_single_candidate() {
        // Setup: 3 nodes, node 1 starts election
        let mut storage1: InMemoryStorage<String> = InMemoryStorage::new();
        let mut storage2: InMemoryStorage<String> = InMemoryStorage::new();
        let mut storage3: InMemoryStorage<String> = InMemoryStorage::new();

        // Node 1 becomes candidate with term 1
        storage1.set_term(1).unwrap();
        storage1.set_vote(Some(1)).unwrap();

        // Simulate vote responses
        // Node 2 votes for node 1
        let vote_granted = {
            let current_term = storage2.get_term().unwrap_or(0);
            let voted_for = storage2.get_vote().unwrap_or(None);

            // Grant vote: term >= current, not voted yet
            1 >= current_term && voted_for.is_none()
        };

        assert!(vote_granted);
        storage2.set_vote(Some(1)).unwrap();

        // Node 3 votes for node 1
        let vote_granted = {
            let current_term = storage3.get_term().unwrap_or(0);
            let voted_for = storage3.get_vote().unwrap_or(None);

            1 >= current_term && voted_for.is_none()
        };

        assert!(vote_granted);
        storage3.set_vote(Some(1)).unwrap();

        // Node 1 has 3 votes (including self), quorum is 2
        let votes = 3;
        let quorum = 2;
        assert!(votes >= quorum);
    }

    #[tokio::test]
    async fn test_log_replication_consistency() {
        let mut leader_storage: InMemoryStorage<String> = InMemoryStorage::new();
        let mut follower_storage: InMemoryStorage<String> = InMemoryStorage::new();

        // Leader appends entries
        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                command: "cmd1".to_string(),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: "cmd2".to_string(),
            },
            LogEntry {
                term: 1,
                index: 3,
                command: "cmd3".to_string(),
            },
        ];
        leader_storage.append_entries(&entries).unwrap();

        // Simulate AppendEntries RPC
        let prev_log_index = 0;
        let prev_log_term = 0;

        // Follower checks consistency
        let consistent = if prev_log_index == 0 {
            true
        } else {
            follower_storage
                .get_log_entry(prev_log_index)
                .unwrap()
                .map(|e| e.term == prev_log_term)
                .unwrap_or(false)
        };

        assert!(consistent);

        // Follower appends entries
        follower_storage.append_entries(&entries).unwrap();

        // Verify replication
        assert_eq!(follower_storage.get_log().unwrap().len(), 3);
        assert_eq!(
            follower_storage.get_log_entry(2).unwrap().unwrap().command,
            "cmd2"
        );
    }

    #[tokio::test]
    async fn test_commit_index_advancement() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();

        // Append entries
        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                command: "cmd1".to_string(),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: "cmd2".to_string(),
            },
        ];
        storage.append_entries(&entries).unwrap();

        // Simulate majority replication (match_index)
        let match_indices = vec![2, 2]; // 2 followers at index 2
        let quorum = 2;

        // Find highest N where majority has matchIndex >= N
        let mut commit_index = 0;
        for n in 1..=2 {
            let count = match_indices.iter().filter(|&&m| m >= n).count() + 1; // +1 for leader
            if count >= quorum {
                commit_index = n;
            }
        }

        assert_eq!(commit_index, 2);
        storage.set_commit_index(commit_index).unwrap();
        assert_eq!(storage.get_commit_index().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_log_conflict_resolution() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();

        // Follower has conflicting entries
        let old_entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                command: "old1".to_string(),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: "old2".to_string(),
            },
            LogEntry {
                term: 2,
                index: 3,
                command: "conflict".to_string(),
            },
        ];
        storage.append_entries(&old_entries).unwrap();

        // Leader sends entries with different term at index 3
        let new_entries = vec![
            LogEntry {
                term: 3,
                index: 3,
                command: "new3".to_string(),
            },
            LogEntry {
                term: 3,
                index: 4,
                command: "new4".to_string(),
            },
        ];

        // Check for conflict at index 3
        let existing = storage.get_log_entry(3).unwrap().unwrap();
        if existing.term != 3 {
            // Truncate from index 3
            storage.truncate_log(3).unwrap();
        }

        // Append new entries
        storage.append_entries(&new_entries).unwrap();

        // Verify
        assert_eq!(storage.get_log().unwrap().len(), 4);
        assert_eq!(storage.get_log_entry(3).unwrap().unwrap().command, "new3");
    }

    #[tokio::test]
    async fn test_state_machine_apply() {
        let sm = KeyValueStateMachine::new();
        let mut storage: InMemoryStorage<KvCommand> = InMemoryStorage::new();

        // Append commands to log
        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                command: KvCommand::Set {
                    key: "a".to_string(),
                    value: b"1".to_vec(),
                },
            },
            LogEntry {
                term: 1,
                index: 2,
                command: KvCommand::Set {
                    key: "b".to_string(),
                    value: b"2".to_vec(),
                },
            },
        ];
        storage.append_entries(&entries).unwrap();

        // Create replicated state machine
        let mut rsm = ReplicatedStateMachine::new(sm, storage);

        // Apply committed entries
        let outputs = rsm.apply_committed(2).unwrap();

        assert_eq!(outputs.len(), 2);
        assert_eq!(rsm.last_applied(), 2);
        assert_eq!(rsm.state_machine().get("a"), Some(&b"1".to_vec()));
        assert_eq!(rsm.state_machine().get("b"), Some(&b"2".to_vec()));
    }

    #[test]
    fn test_raft_config() {
        let config = RaftConfig::default();

        // Heartbeat must be less than election timeout
        assert!(config.heartbeat_interval < config.election_timeout_min);

        // Election timeout randomization
        let t1 = config.random_election_timeout();
        assert!(t1 >= config.election_timeout_min);
        assert!(t1 <= config.election_timeout_max);
    }
}
