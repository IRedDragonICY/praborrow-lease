use crate::network::ConsensusNetwork;
use crate::raft::{NodeId, Term, RaftStorage};
use async_trait::async_trait;
use std::boxed::Box;
use serde::{Serialize, de::DeserializeOwned};

/// Types of consensus algorithms supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusStrategy {
    /// Raft consensus algorithm
    Raft,
    /// Paxos consensus algorithm (future work)
    Paxos,
}

/// Abstract interface for a consensus engine.
///
/// This trait provides a unified interface for different consensus algorithms.
#[async_trait]
pub trait ConsensusEngine<T>: Send {
    /// Starts the consensus loop.
    ///
    /// This method runs the consensus algorithm's main loop and should
    /// typically be spawned on a separate task.
    async fn run(&mut self) -> Result<(), ConsensusError>;
    
    /// Proposes a new value to be agreed upon.
    ///
    /// Returns the term at which the value was proposed.
    async fn propose(&mut self, value: T) -> Result<Term, ConsensusError>;
    
    /// Returns the current leader ID, if known.
    fn leader_id(&self) -> Option<NodeId>;
}

/// Error type for consensus engine operations.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ConsensusError {
    /// The requested consensus strategy is not yet implemented.
    #[error("Consensus strategy {0:?} is not yet implemented")]
    NotImplemented(ConsensusStrategy),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Not leader")]
    NotLeader,
    #[error("Term mismatch")]
    TermMismatch,
    /// Snapshot operation failed
    #[error("Snapshot error: {0}")]
    SnapshotError(String),
    /// Log compaction failed
    #[error("Compaction error: {0}")]
    CompactionError(String),
    /// Data integrity check failed
    #[error("Integrity check failed: {0}")]
    IntegrityError(String),
    /// Log index is out of bounds
    #[error("Index out of bounds: requested {requested}, available {available}")]
    IndexOutOfBounds { requested: u64, available: u64 },
}

/// Factory for creating consensus engines.
///
/// Provides a unified way to create different consensus engine implementations.
pub struct ConsensusFactory;

impl ConsensusFactory {
    /// Creates a new consensus engine with the specified strategy.
    ///
    /// # Arguments
    ///
    /// * `strategy` - The consensus algorithm to use
    /// * `id` - Unique node identifier
    /// * `network` - Network transport for consensus messages  
    /// * `storage` - Storage backend for persistent state
    ///
    /// # Returns
    ///
    /// A boxed consensus engine, or an error if the strategy is not implemented.
    pub fn create_engine<T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static>(
        strategy: ConsensusStrategy,
        id: NodeId,
        network: Box<dyn ConsensusNetwork>,
        storage: Box<dyn RaftStorage<T>>,
    ) -> Result<Box<dyn ConsensusEngine<T>>, ConsensusError> {
        tracing::info!(
            strategy = ?strategy,
            node_id = id,
            "Creating consensus engine"
        );

        match strategy {
            ConsensusStrategy::Raft => {
                // Create Raft node and wrap it
                Ok(Box::new(RaftEngineAdapter::new(id, network, storage)))
            }
            ConsensusStrategy::Paxos => {
                tracing::error!(
                    strategy = ?strategy,
                    "Paxos consensus strategy is not yet implemented"
                );
                Err(ConsensusError::NotImplemented(ConsensusStrategy::Paxos))
            }
        }
    }
}

/// Adapter that wraps RaftNode to implement ConsensusEngine.
struct RaftEngineAdapter<T> {
    node: crate::raft::RaftNode<T>,
}

impl<T: Clone + Send + Serialize + DeserializeOwned + 'static> RaftEngineAdapter<T> {
    fn new(
        id: NodeId,
        network: Box<dyn ConsensusNetwork>,
        storage: Box<dyn RaftStorage<T>>,
    ) -> Self {
        Self {
            node: crate::raft::RaftNode::new(id, network, storage),
        }
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static> ConsensusEngine<T> for RaftEngineAdapter<T> {
    async fn run(&mut self) -> Result<(), ConsensusError> {
        tracing::info!(
            node_id = self.node.id,
            "Starting Raft consensus loop"
        );
        
        // Main consensus loop (simplified)
        loop {
            // In a real implementation, this would:
            // 1. Handle incoming RPCs
            // 2. Send heartbeats (if leader)
            // 3. Start elections (if timeout)
            // 4. Apply committed entries
            
            // For now, just yield to avoid busy-looping
            tokio::task::yield_now().await;
        }
    }
    
    async fn propose(&mut self, _value: T) -> Result<Term, ConsensusError> {
        // In a real implementation, this would append to the log
        // and replicate to followers
        let term = self.node.storage.get_term().unwrap_or(0);
        tracing::debug!(
            node_id = self.node.id,
            term = term,
            "Proposal received (stub)"
        );
        Ok(term)
    }
    
    fn leader_id(&self) -> Option<NodeId> {
        // In a real implementation, track the current leader
        // For now, return self if leader
        if self.node.role == crate::raft::RaftRole::Leader {
            Some(self.node.id)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::InMemoryStorage;

    // Mock network for testing
    struct MockNetwork;
    
    #[async_trait]
    impl ConsensusNetwork for MockNetwork {
        async fn broadcast_vote_request(&self, _term: Term, _candidate_id: NodeId) -> Result<(), String> {
            Ok(())
        }
        
        async fn send_heartbeat(&self, _leader_id: NodeId, _term: Term) -> Result<(), String> {
            Ok(())
        }
        
        async fn receive(&self) -> Result<crate::network::Packet, String> {
            // Block forever in tests
            futures::future::pending().await
        }

        async fn update_peers(&self, _peers: Vec<String>) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn test_create_raft_engine() {
        let storage: Box<dyn RaftStorage<String>> = Box::new(InMemoryStorage::new());
        let network: Box<dyn ConsensusNetwork> = Box::new(MockNetwork);
        
        let result = ConsensusFactory::create_engine(
            ConsensusStrategy::Raft,
            1,
            network,
            storage,
        );
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_paxos_not_implemented() {
        let storage: Box<dyn RaftStorage<String>> = Box::new(InMemoryStorage::new());
        let network: Box<dyn ConsensusNetwork> = Box::new(MockNetwork);
        
        let result = ConsensusFactory::create_engine(
            ConsensusStrategy::Paxos,
            1,
            network,
            storage,
        );
        
        assert!(matches!(result, Err(ConsensusError::NotImplemented(_))));
    }
}

