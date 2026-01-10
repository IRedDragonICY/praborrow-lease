use serde::{Serialize, Deserialize};
use crate::network::ConsensusNetwork;
use std::path::PathBuf;
use crate::engine::ConsensusError;

pub type Term = u64;
pub type NodeId = u128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Display for RaftRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftRole::Follower => write!(f, "Follower"),
            RaftRole::Candidate => write!(f, "Candidate"),
            RaftRole::Leader => write!(f, "Leader"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub term: Term,
    pub command: T,
}

/// Abstract storage interface for Raft state persistence.
///
/// This trait allows swapping storage backends (in-memory, file-based, etc.)
/// without changing the Raft implementation.
pub trait RaftStorage<T>: Send {
    /// Appends entries to the log.
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError>;
    
    /// Gets the current term.
    fn get_term(&self) -> Result<Term, ConsensusError>;
    
    /// Sets the current term.
    fn set_term(&mut self, term: Term) -> Result<(), ConsensusError>;
    
    /// Gets the voted-for candidate.
    fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError>;
    
    /// Sets the voted-for candidate.
    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError>;
    
    /// Gets a reference to the log.
    /// Gets the log entries.
    fn get_log(&self) -> Result<Vec<LogEntry<T>>, ConsensusError>;

    /// Gets the current peer configuration.
    fn get_peers(&self) -> Result<Vec<String>, ConsensusError>;

    /// Sets the current peer configuration.
    fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError>;
}

/// Default in-memory storage implementation.
///
/// This storage is volatile - all data is lost on restart.
/// Use `FileStorage` for persistent storage.
pub struct InMemoryStorage<T> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<T>>,
    peers: Vec<String>,
}

impl<T> InMemoryStorage<T> {
    pub fn new() -> Self {
        tracing::debug!("Creating new in-memory Raft storage");
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            peers: Vec::new(),
        }
    }
}

impl<T> Default for InMemoryStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send> RaftStorage<T> for InMemoryStorage<T> {
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError> {
        tracing::trace!(
            entry_count = entries.len(),
            "Appending entries to in-memory log"
        );
        self.log.extend_from_slice(entries);
        Ok(())
    }

    fn get_term(&self) -> Result<Term, ConsensusError> {
        Ok(self.current_term)
    }

    fn set_term(&mut self, term: Term) -> Result<(), ConsensusError> {
        tracing::debug!(
            old_term = self.current_term,
            new_term = term,
            "Setting term"
        );
        self.current_term = term;
        Ok(())
    }

    fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError> {
        Ok(self.voted_for)
    }

    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError> {
        tracing::debug!(
            old_vote = ?self.voted_for,
            new_vote = ?vote,
            "Setting vote"
        );
        self.voted_for = vote;
        Ok(())
    }

    fn get_log(&self) -> Result<Vec<LogEntry<T>>, ConsensusError> {
        Ok(self.log.clone())
    }

    fn get_peers(&self) -> Result<Vec<String>, ConsensusError> {
        Ok(self.peers.clone())
    }

    fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError> {
        self.peers = peers.to_vec();
        Ok(())
    }
}

/// File-based storage implementation (stub).
///
/// This struct is a placeholder for persistent storage implementation.
/// Currently it wraps in-memory storage but provides the structure
/// for future file-based persistence.
///
/// # Future Implementation
///
/// The full implementation would:
/// 1. Write term and vote to a metadata file on each update
/// 2. Append log entries to a log file
/// 3. Support log compaction and snapshotting
/// 4. Handle crash recovery
pub struct FileStorage<T> {
    /// Path to the storage directory
    #[allow(dead_code)]
    path: PathBuf,
    /// Sled database instance
    db: sled::Db,
    /// Phantom data for T
    _phantom: std::marker::PhantomData<T>,
}

impl<T> FileStorage<T> {
    pub fn new(path: PathBuf) -> Self {
        tracing::info!(
            path = %path.display(),
            "Opening persistent Raft storage"
        );
        
        let db = sled::open(&path).expect("Failed to open sled database");
        
        Self {
            path,
            db,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Clone + Send + serde::Serialize + serde::de::DeserializeOwned> RaftStorage<T> for FileStorage<T> {
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError> {
        let log_tree = self.db.open_tree("log").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        
        // Find the next index (simplified: just count keys for now, or use autoincrement idea)
        // For a real Raft, we need accurate indices. 
        // Assuming append is always at the end for this simple impl, or we need to know the current index.
        // Let's rely on the current count + 1.
        let mut current_idx = log_tree.len() as u64;

        for entry in entries {
            let key = current_idx.to_be_bytes();
            let value = bincode::serialize(entry).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            log_tree.insert(key, value).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            current_idx += 1;
        }
        
        self.db.flush().map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }

    fn get_term(&self) -> Result<Term, ConsensusError> {
        let meta = self.db.open_tree("meta").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        if let Some(val) = meta.get(b"term").map_err(|e| ConsensusError::StorageError(e.to_string()))? {
            let term: Term = bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            Ok(term)
        } else {
            Ok(0)
        }
    }

    fn set_term(&mut self, term: Term) -> Result<(), ConsensusError> {
        let meta = self.db.open_tree("meta").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let val = bincode::serialize(&term).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        meta.insert(b"term", val).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.db.flush().map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }

    fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError> {
        let meta = self.db.open_tree("meta").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        if let Some(val) = meta.get(b"vote").map_err(|e| ConsensusError::StorageError(e.to_string()))? {
            let vote: Option<NodeId> = bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            Ok(vote)
        } else {
            Ok(None)
        }
    }

    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError> {
        let meta = self.db.open_tree("meta").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let val = bincode::serialize(&vote).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        meta.insert(b"vote", val).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.db.flush().map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }

    fn get_log(&self) -> Result<Vec<LogEntry<T>>, ConsensusError> {
        let log_tree = self.db.open_tree("log").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let mut entries = Vec::new();
        for item in log_tree.iter() {
            let (_, value) = item.map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            let entry: LogEntry<T> = bincode::deserialize(&value).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    fn get_peers(&self) -> Result<Vec<String>, ConsensusError> {
        let meta = self.db.open_tree("meta").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        if let Some(val) = meta.get(b"peers").map_err(|e| ConsensusError::StorageError(e.to_string()))? {
            let peers: Vec<String> = bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            Ok(peers)
        } else {
            Ok(Vec::new())
        }
    }

    fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError> {
        let meta = self.db.open_tree("meta").map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let val = bincode::serialize(peers).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        meta.insert(b"peers", val).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.db.flush().map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }
}

/// The Raft State Machine.
///
/// Implements the Raft consensus algorithm for distributed agreement.
pub struct RaftNode<T> {
    // Persistent State abstracted via Storage
    pub storage: Box<dyn RaftStorage<T>>,

    // Volatile State
    pub commit_index: usize,
    pub last_applied: usize,
    
    // Node State
    pub role: RaftRole,
    pub id: NodeId,
    
    // Networking
    pub network: Box<dyn ConsensusNetwork>,
}

impl<T: Clone + Send + 'static> RaftNode<T> {
    /// Creates a new Raft node with custom storage.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this node
    /// * `network` - Network transport for consensus messages
    /// * `storage` - Storage backend for persisting Raft state
    ///
    /// # Example
    ///
    /// ```ignore
    /// use praborrow_lease::raft::{RaftNode, InMemoryStorage, FileStorage};
    ///
    /// // In-memory storage (volatile)
    /// let node = RaftNode::new(1, network, Box::new(InMemoryStorage::new()));
    ///
    /// // File-based storage (persistent)
    /// let node = RaftNode::new(1, network, Box::new(FileStorage::new("./raft-data".into())));
    /// ```
    pub fn new(
        id: NodeId,
        network: Box<dyn ConsensusNetwork>,
        storage: Box<dyn RaftStorage<T>>,
    ) -> Self {
        tracing::info!(
            node_id = id,
            "Creating new Raft node"
        );
        Self {
            storage,
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            id,
            network,
        }
    }

    /// Creates a new Raft node with default in-memory storage.
    ///
    /// Convenience method for testing and development.
    /// For production, use `new()` with a persistent storage backend.
    pub fn with_memory_storage(id: NodeId, network: Box<dyn ConsensusNetwork>) -> Self {
        Self::new(id, network, Box::new(InMemoryStorage::new()))
    }

    /// Transition to Candidate and start election.
    pub async fn start_election(&mut self) {
        let current_term = self.storage.get_term().unwrap_or(0);
        let new_term = current_term + 1;
        
        let _ = self.storage.set_term(new_term);
        let _ = self.storage.set_vote(Some(self.id));
        
        let old_role = self.role.clone();
        self.role = RaftRole::Candidate;
        
        tracing::info!(
            node_id = self.id,
            from_role = %old_role,
            to_role = %self.role,
            term = new_term,
            "Starting election"
        );
        
        // Broadcast RequestVote
        if let Err(e) = self.network.broadcast_vote_request(new_term, self.id).await {
            tracing::error!(
                node_id = self.id,
                error = %e,
                "Failed to broadcast vote request"
            );
        }
    }

    /// Handle RequestVote RPC.
    pub fn handle_request_vote(&mut self, term: Term, candidate_id: NodeId) -> bool {
        let current_term = self.storage.get_term().unwrap_or(0);
        let voted_for = self.storage.get_vote().unwrap_or(None);

        if term > current_term {
            tracing::debug!(
                node_id = self.id,
                old_term = current_term,
                new_term = term,
                "Received higher term, stepping down"
            );
            let _ = self.storage.set_term(term);
            if self.role != RaftRole::Follower {
                tracing::info!(
                    node_id = self.id,
                    from_role = %self.role,
                    to_role = "Follower",
                    "Role transition"
                );
            }
            self.role = RaftRole::Follower;
            let _ = self.storage.set_vote(None);
        }

        if term < current_term {
            tracing::debug!(
                node_id = self.id,
                request_term = term,
                current_term = current_term,
                candidate_id = candidate_id,
                "Rejecting vote request due to stale term"
            );
            return false;
        }

        if voted_for.is_none() || voted_for == Some(candidate_id) {
            tracing::debug!(
                node_id = self.id,
                term = term,
                candidate_id = candidate_id,
                "Granting vote"
            );
            let _ = self.storage.set_vote(Some(candidate_id));
            return true;
        }

        tracing::debug!(
            node_id = self.id,
            term = term,
            candidate_id = candidate_id,
            already_voted_for = ?voted_for,
            "Rejecting vote request (already voted)"
        );
        false
    }
    
    /// Adds a peer to the cluster configuration.
    ///
    /// Updates both persistent storage and the active network transport.
    pub async fn add_node(&mut self, peer_address: String) -> Result<(), ConsensusError> {
        let mut peers = self.storage.get_peers()?;
        if !peers.contains(&peer_address) {
            peers.push(peer_address.clone());
            self.storage.set_peers(&peers)?;
            
            // Notify network layer
            if let Err(e) = self.network.update_peers(peers).await {
                return Err(ConsensusError::NetworkError(e));
            }
            
            tracing::info!(node_id = self.id, peer = peer_address, "Added node to cluster");
        }
        Ok(())
    }

    /// Removes a peer from the cluster configuration.
    pub async fn remove_node(&mut self, peer_address: &str) -> Result<(), ConsensusError> {
        let mut peers = self.storage.get_peers()?;
        if let Some(pos) = peers.iter().position(|p| p == peer_address) {
            peers.remove(pos);
            self.storage.set_peers(&peers)?;
            
            // Notify network layer
            if let Err(e) = self.network.update_peers(peers).await {
                return Err(ConsensusError::NetworkError(e));
            }

            tracing::info!(node_id = self.id, peer = peer_address, "Removed node from cluster");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_storage() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();
        
        assert_eq!(storage.get_term().unwrap(), 0);
        storage.set_term(5).unwrap();
        assert_eq!(storage.get_term().unwrap(), 5);
        
        assert_eq!(storage.get_vote().unwrap(), None);
        storage.set_vote(Some(42)).unwrap();
        assert_eq!(storage.get_vote().unwrap(), Some(42));
        
        let entry = LogEntry { term: 1, command: "test".to_string() };
        storage.append_entries(&[entry]).unwrap();
        assert_eq!(storage.get_log().unwrap().len(), 1);
    }

    #[test]
    fn test_file_storage_persistence() {
        let path = std::env::temp_dir().join("raft_test_storage");
        // Clean up previous run
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        {
            let mut storage: FileStorage<String> = FileStorage::new(path.clone());
            storage.set_term(10).unwrap();
            storage.set_vote(Some(1)).unwrap();
            
            let entry = LogEntry { term: 1, command: "test_persist".to_string() };
            storage.append_entries(&[entry]).unwrap();
        }

        // Re-open
        {
            let storage: FileStorage<String> = FileStorage::new(path.clone());
            assert_eq!(storage.get_term().unwrap(), 10);
            assert_eq!(storage.get_vote().unwrap(), Some(1));
            
            let log = storage.get_log().unwrap();
            assert_eq!(log.len(), 1);
            assert_eq!(log[0].command, "test_persist");
        }
        
        // Cleanup
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_raft_role_display() {
        assert_eq!(RaftRole::Follower.to_string(), "Follower");
        assert_eq!(RaftRole::Candidate.to_string(), "Candidate");
        assert_eq!(RaftRole::Leader.to_string(), "Leader");
    }

    #[tokio::test]
    async fn test_raft_node_dynamic_membership() {
        use crate::network::Packet;
        use async_trait::async_trait;

        // Mock network
        struct MockNetwork {
            peers: std::sync::Arc<std::sync::RwLock<Vec<String>>>,
        }
        #[async_trait]
        impl ConsensusNetwork for MockNetwork {
            async fn broadcast_vote_request(&self, _term: Term, _candidate_id: NodeId) -> Result<(), String> { Ok(()) }
            async fn send_heartbeat(&self, _leader_id: NodeId, _term: Term) -> Result<(), String> { Ok(()) }
            async fn receive(&self) -> Result<Packet, String> { futures::future::pending().await }
            async fn update_peers(&self, peers: Vec<String>) -> Result<(), String> {
                *self.peers.write().unwrap() = peers;
                Ok(())
            }
        }
        
        let mock_network = Box::new(MockNetwork { peers: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())) });
        // We need a handle to verify network updates, but Box<dyn> consumes it.
        // So we'll trust the interaction or do a more complex setup if needed.
        // Actually, we can use a shared state.
        
        let shared_peers = std::sync::Arc::new(std::sync::RwLock::new(Vec::new()));
        struct SharedMockNetwork(std::sync::Arc<std::sync::RwLock<Vec<String>>>);
        #[async_trait]
        impl ConsensusNetwork for SharedMockNetwork {
            async fn broadcast_vote_request(&self, _t: Term, _c: NodeId) -> Result<(), String> { Ok(()) }
            async fn send_heartbeat(&self, _l: NodeId, _t: Term) -> Result<(), String> { Ok(()) }
            async fn receive(&self) -> Result<Packet, String> { futures::future::pending().await }
            async fn update_peers(&self, peers: Vec<String>) -> Result<(), String> {
                *self.0.write().unwrap() = peers;
                Ok(())
            }
        }
        
        let network = Box::new(SharedMockNetwork(shared_peers.clone()));
        let mut node = RaftNode::new(1, network, Box::new(InMemoryStorage::<String>::new()));
        
        // Initial state
        assert!(node.storage.get_peers().unwrap().is_empty());
        assert!(shared_peers.read().unwrap().is_empty());
        
        // Add node
        node.add_node("192.168.1.10:8000".to_string()).await.unwrap();
        
        // Verify storage
        let peers = node.storage.get_peers().unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], "192.168.1.10:8000");
        
        // Verify network
        assert_eq!(shared_peers.read().unwrap().len(), 1);
        
        // Add duplicate (should be ignored)
        node.add_node("192.168.1.10:8000".to_string()).await.unwrap();
         let peers = node.storage.get_peers().unwrap();
        assert_eq!(peers.len(), 1);
        
        // Remove node
        node.remove_node("192.168.1.10:8000").await.unwrap();
        assert!(node.storage.get_peers().unwrap().is_empty());
        assert!(shared_peers.read().unwrap().is_empty());
    }
}
