use serde::{Serialize, Deserialize};
use crate::network::ConsensusNetwork;
use std::boxed::Box;
use std::path::PathBuf;

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
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), String>;
    
    /// Gets the current term.
    fn get_term(&self) -> Result<Term, String>;
    
    /// Sets the current term.
    fn set_term(&mut self, term: Term) -> Result<(), String>;
    
    /// Gets the voted-for candidate.
    fn get_vote(&self) -> Result<Option<NodeId>, String>;
    
    /// Sets the voted-for candidate.
    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), String>;
    
    /// Gets a reference to the log.
    fn get_log(&self) -> Result<&[LogEntry<T>], String>;
}

/// Default in-memory storage implementation.
///
/// This storage is volatile - all data is lost on restart.
/// Use `FileStorage` for persistent storage.
pub struct InMemoryStorage<T> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<T>>,
}

impl<T> InMemoryStorage<T> {
    pub fn new() -> Self {
        tracing::debug!("Creating new in-memory Raft storage");
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl<T> Default for InMemoryStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send> RaftStorage<T> for InMemoryStorage<T> {
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), String> {
        tracing::trace!(
            entry_count = entries.len(),
            "Appending entries to in-memory log"
        );
        self.log.extend_from_slice(entries);
        Ok(())
    }

    fn get_term(&self) -> Result<Term, String> {
        Ok(self.current_term)
    }

    fn set_term(&mut self, term: Term) -> Result<(), String> {
        tracing::debug!(
            old_term = self.current_term,
            new_term = term,
            "Setting term"
        );
        self.current_term = term;
        Ok(())
    }

    fn get_vote(&self) -> Result<Option<NodeId>, String> {
        Ok(self.voted_for)
    }

    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), String> {
        tracing::debug!(
            old_vote = ?self.voted_for,
            new_vote = ?vote,
            "Setting vote"
        );
        self.voted_for = vote;
        Ok(())
    }

    fn get_log(&self) -> Result<&[LogEntry<T>], String> {
        Ok(&self.log)
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
    /// In-memory cache of the state (backed by files in full impl)
    inner: InMemoryStorage<T>,
}

impl<T> FileStorage<T> {
    /// Creates a new file-based storage at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path where state files will be stored
    ///
    /// # Note
    ///
    /// This is currently a stub that uses in-memory storage internally.
    /// Future versions will persist state to disk.
    pub fn new(path: PathBuf) -> Self {
        tracing::info!(
            path = %path.display(),
            "Creating file-based Raft storage (stub - data not persisted)"
        );
        Self {
            path,
            inner: InMemoryStorage::new(),
        }
    }
}

impl<T: Clone + Send> RaftStorage<T> for FileStorage<T> {
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), String> {
        // TODO: Persist to file
        self.inner.append_entries(entries)
    }

    fn get_term(&self) -> Result<Term, String> {
        self.inner.get_term()
    }

    fn set_term(&mut self, term: Term) -> Result<(), String> {
        // TODO: Persist to file
        self.inner.set_term(term)
    }

    fn get_vote(&self) -> Result<Option<NodeId>, String> {
        self.inner.get_vote()
    }

    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), String> {
        // TODO: Persist to file
        self.inner.set_vote(vote)
    }

    fn get_log(&self) -> Result<&[LogEntry<T>], String> {
        self.inner.get_log()
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
    fn test_file_storage_stub() {
        let mut storage: FileStorage<String> = FileStorage::new(PathBuf::from("/tmp/raft-test"));
        
        // Should work like in-memory storage (stub behavior)
        storage.set_term(10).unwrap();
        assert_eq!(storage.get_term().unwrap(), 10);
    }

    #[test]
    fn test_raft_role_display() {
        assert_eq!(RaftRole::Follower.to_string(), "Follower");
        assert_eq!(RaftRole::Candidate.to_string(), "Candidate");
        assert_eq!(RaftRole::Leader.to_string(), "Leader");
    }
}
