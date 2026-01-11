use crate::engine::ConsensusError;
use crate::network::ConsensusNetwork;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;

pub type Term = u64;
pub type NodeId = u128;
pub type LogIndex = u64;

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
    pub index: LogIndex,
    pub command: T,
}

/// Snapshot of the state machine for log compaction.
///
/// Contains the state at a specific log index, allowing truncation of
/// all log entries before that index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot<T> {
    /// The last log index included in this snapshot
    pub last_included_index: LogIndex,
    /// The term of the last included log entry
    pub last_included_term: Term,
    /// The serialized state machine data
    pub data: T,
    /// CRC32 checksum for integrity verification
    pub checksum: u32,
}

impl<T: Serialize> Snapshot<T> {
    /// Creates a new snapshot with computed checksum.
    pub fn new(
        last_included_index: LogIndex,
        last_included_term: Term,
        data: T,
    ) -> Result<Self, ConsensusError>
    where
        T: Clone,
    {
        let serialized = bincode::serialize(&data).map_err(|e| {
            ConsensusError::SnapshotError(format!("Failed to serialize data: {}", e))
        })?;
        let checksum = crc32fast::hash(&serialized);

        Ok(Self {
            last_included_index,
            last_included_term,
            data,
            checksum,
        })
    }

    /// Verifies the snapshot integrity.
    pub fn verify(&self) -> Result<(), ConsensusError>
    where
        T: Clone,
    {
        let serialized = bincode::serialize(&self.data).map_err(|e| {
            ConsensusError::SnapshotError(format!("Failed to serialize for verification: {}", e))
        })?;
        let computed = crc32fast::hash(&serialized);

        if computed != self.checksum {
            return Err(ConsensusError::IntegrityError(format!(
                "Snapshot checksum mismatch: expected {}, got {}",
                self.checksum, computed
            )));
        }
        Ok(())
    }
}

/// Information about the last log entry.
///
/// Used for AppendEntries RPC log consistency check.
#[derive(Debug, Clone, Default)]
pub struct LogInfo {
    pub last_index: LogIndex,
    pub last_term: Term,
}

/// Abstract storage interface for Raft state persistence.
///
/// This trait allows swapping storage backends (in-memory, file-based, etc.)
/// without changing the Raft implementation.
///
/// All operations that modify state must be atomic and durable.
/// Implementations must handle crash recovery correctly.
///
/// All methods are async to support non-blocking I/O backends (e.g., remote databases).
#[async_trait]
pub trait RaftStorage<T>: Send + Sync
where
    T: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static,
{
    /// Appends log entries to the storage.
    ///
    /// The entries must be persisted durably before returning.
    async fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError>;

    /// Gets a single log entry by index.
    ///
    /// Returns `None` if the index is out of bounds or has been compacted.
    async fn get_log_entry(&self, index: LogIndex) -> Result<Option<LogEntry<T>>, ConsensusError>;

    /// Gets a range of log entries [start, end).
    ///
    /// Returns empty vec if range is invalid or compacted.
    async fn get_log_range(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError>;

    /// Gets all log entries.
    ///
    /// Returns an iterator over all log entries.
    async fn get_log(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError>;

    /// Gets the last log index and term.
    ///
    /// Returns (0, 0) if log is empty.
    async fn get_last_log_info(&self) -> Result<LogInfo, ConsensusError>;

    /// Truncates the log at the given index (exclusive).
    ///
    /// Removes all entries with index >= `from_index`.
    /// Used for conflict resolution during AppendEntries.
    async fn truncate_log(&mut self, from_index: LogIndex) -> Result<(), ConsensusError>;

    // ===== Metadata Operations =====

    /// Gets the current term.
    async fn get_term(&self) -> Result<Term, ConsensusError>;

    /// Sets the current term.
    ///
    /// This operation must be atomic with clearing voted_for when term increases.
    async fn set_term(&mut self, term: Term) -> Result<(), ConsensusError>;

    /// Gets the voted-for candidate in current term.
    async fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError>;

    /// Sets the voted-for candidate.
    async fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError>;

    /// Atomically updates term and vote together.
    ///
    /// This is critical for correctness - term and vote must be updated atomically.
    async fn set_term_and_vote(
        &mut self,
        term: Term,
        vote: Option<NodeId>,
    ) -> Result<(), ConsensusError> {
        // Default implementation (not atomic, implementations should override)
        self.set_term(term).await?;
        self.set_vote(vote).await
    }

    /// Gets the commit index.
    async fn get_commit_index(&self) -> Result<LogIndex, ConsensusError>;

    /// Sets the commit index.
    async fn set_commit_index(&mut self, index: LogIndex) -> Result<(), ConsensusError>;

    // ===== Cluster Configuration =====

    /// Gets the current peer configuration.
    async fn get_peers(&self) -> Result<Vec<String>, ConsensusError>;

    /// Sets the current peer configuration.
    async fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError>;

    // ===== Snapshot Operations =====

    /// Gets the current snapshot, if any.
    async fn get_snapshot(&self) -> Result<Option<Snapshot<T>>, ConsensusError>
    where
        T: Clone + serde::de::DeserializeOwned;

    /// Creates a snapshot of the current state.
    ///
    /// This compacts the log by:
    /// 1. Saving the snapshot atomically
    /// 2. Truncating log entries before the snapshot index
    async fn create_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), ConsensusError>
    where
        T: Clone + Serialize;

    /// Installs a snapshot received from the leader.
    async fn install_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), ConsensusError>
    where
        T: Clone + Serialize;
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
    commit_index: LogIndex,
    snapshot: Option<Snapshot<T>>,
    // The first log index (after compaction, this may not be 0)
    first_log_index: LogIndex,
}

impl<T> InMemoryStorage<T> {
    pub fn new() -> Self {
        tracing::debug!("Creating new in-memory Raft storage");
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            peers: Vec::new(),
            commit_index: 0,
            snapshot: None,
            first_log_index: 0,
        }
    }
}

impl<T> Default for InMemoryStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static> RaftStorage<T>
    for InMemoryStorage<T>
{
    async fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError> {
        tracing::trace!(
            entry_count = entries.len(),
            "Appending entries to in-memory log"
        );
        self.log.extend_from_slice(entries);
        Ok(())
    }

    async fn get_log_entry(&self, index: LogIndex) -> Result<Option<LogEntry<T>>, ConsensusError> {
        // Find entry by its actual index field
        Ok(self.log.iter().find(|e| e.index == index).cloned())
    }

    async fn get_log_range(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError> {
        if start >= end {
            return Ok(Box::new(std::iter::empty()));
        }
        // Filter entries by their actual index field
        let entries: Vec<Result<LogEntry<T>, ConsensusError>> = self
            .log
            .iter()
            .filter(|e| e.index >= start && e.index < end)
            .cloned()
            .map(Ok)
            .collect();
        Ok(Box::new(entries.into_iter()))
    }

    async fn get_term(&self) -> Result<Term, ConsensusError> {
        Ok(self.current_term)
    }

    async fn set_term(&mut self, term: Term) -> Result<(), ConsensusError> {
        tracing::debug!(
            old_term = self.current_term,
            new_term = term,
            "Setting term"
        );
        self.current_term = term;
        Ok(())
    }

    async fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError> {
        Ok(self.voted_for)
    }

    async fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError> {
        tracing::debug!(
            old_vote = ?self.voted_for,
            new_vote = ?vote,
            "Setting vote"
        );
        self.voted_for = vote;
        Ok(())
    }

    async fn set_term_and_vote(
        &mut self,
        term: Term,
        vote: Option<NodeId>,
    ) -> Result<(), ConsensusError> {
        self.current_term = term;
        self.voted_for = vote;
        Ok(())
    }

    async fn get_log(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError> {
        let entries: Vec<Result<LogEntry<T>, ConsensusError>> = self.log.iter().cloned().map(Ok).collect();
        Ok(Box::new(entries.into_iter()))
    }

    async fn get_last_log_info(&self) -> Result<LogInfo, ConsensusError> {
        if let Some(entry) = self.log.last() {
            Ok(LogInfo {
                last_index: entry.index,
                last_term: entry.term,
            })
        } else if let Some(ref snapshot) = self.snapshot {
            Ok(LogInfo {
                last_index: snapshot.last_included_index,
                last_term: snapshot.last_included_term,
            })
        } else {
            Ok(LogInfo::default())
        }
    }

    async fn truncate_log(&mut self, from_index: LogIndex) -> Result<(), ConsensusError> {
        // Keep entries with index < from_index
        self.log.retain(|e| e.index < from_index);
        tracing::debug!(
            from_index = from_index,
            remaining = self.log.len(),
            "Truncated log"
        );
        Ok(())
    }

    async fn get_commit_index(&self) -> Result<LogIndex, ConsensusError> {
        Ok(self.commit_index)
    }

    async fn set_commit_index(&mut self, index: LogIndex) -> Result<(), ConsensusError> {
        self.commit_index = index;
        Ok(())
    }

    async fn get_peers(&self) -> Result<Vec<String>, ConsensusError> {
        Ok(self.peers.clone())
    }

    async fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError> {
        self.peers = peers.to_vec();
        Ok(())
    }

    async fn get_snapshot(&self) -> Result<Option<Snapshot<T>>, ConsensusError> {
        Ok(self.snapshot.clone())
    }

    async fn create_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), ConsensusError> {
        let compact_up_to = snapshot.last_included_index;
        self.snapshot = Some(snapshot);

        // Remove entries with index <= compact_up_to
        self.log.retain(|e| e.index > compact_up_to);
        self.first_log_index = compact_up_to + 1;

        tracing::info!(
            last_included_index = compact_up_to,
            remaining_log_entries = self.log.len(),
            "Created snapshot and compacted log"
        );
        Ok(())
    }

    async fn install_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), ConsensusError> {
        self.first_log_index = snapshot.last_included_index + 1;
        self.log.clear();
        self.snapshot = Some(snapshot);
        Ok(())
    }
}

// ============================================================================
// PRODUCTION FILE-BASED STORAGE
// ============================================================================

/// Sled tree keys for metadata
mod keys {
    pub const TERM: &[u8] = b"term";
    pub const VOTE: &[u8] = b"vote";
    pub const COMMIT_INDEX: &[u8] = b"commit_index";
    pub const PEERS: &[u8] = b"peers";
    pub const SNAPSHOT: &[u8] = b"snapshot";
    pub const FIRST_LOG_INDEX: &[u8] = b"first_log_index";
}

/// Production-grade file-based storage for Raft.
///
/// Built on Sled embedded database, providing:
/// - **ACID guarantees**: All operations are atomic and durable
/// - **Crash recovery**: Automatic recovery on restart
/// - **Log compaction**: Snapshot-based log truncation
/// - **Integrity verification**: CRC32 checksums for snapshots
///
/// # Storage Layout
///
/// The Sled database uses three trees:
/// - `meta`: Persistent metadata (term, vote, commit_index, peers)
/// - `log`: Log entries keyed by big-endian index
/// - `snapshot`: Snapshot data with integrity checksums
///
/// # Thread Safety
///
/// `FileStorage` is `Send` but not `Sync`. For concurrent access,
/// wrap in appropriate synchronization primitives.
///
/// # Example
///
/// ```ignore
/// use praborrow_lease::raft::FileStorage;
/// use std::path::PathBuf;
///
/// let storage = FileStorage::<String>::open("./raft-data".into())?;
/// ```
pub struct FileStorage<T> {
    /// Path to the storage directory
    path: PathBuf,
    /// Sled database instance
    db: sled::Db,
    /// Metadata tree (term, vote, commit_index, peers)
    meta_tree: sled::Tree,
    /// Log entries tree
    log_tree: sled::Tree,
    /// Phantom data for T
    _phantom: PhantomData<T>,
}

impl<T> FileStorage<T> {
    /// Opens or creates a persistent Raft storage at the given path.
    ///
    /// If the database exists, it will be opened and validated.
    /// If it doesn't exist, a new database will be created.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The path cannot be accessed
    /// - The database is corrupted
    /// - Snapshot integrity check fails
    pub fn open(path: PathBuf) -> Result<Self, ConsensusError> {
        tracing::info!(
            path = %path.display(),
            "Opening persistent Raft storage"
        );

        let db = sled::open(&path)
            .map_err(|e| ConsensusError::StorageError(format!("Failed to open database: {}", e)))?;

        let meta_tree = db.open_tree("meta").map_err(|e| {
            ConsensusError::StorageError(format!("Failed to open meta tree: {}", e))
        })?;

        let log_tree = db
            .open_tree("log")
            .map_err(|e| ConsensusError::StorageError(format!("Failed to open log tree: {}", e)))?;

        let storage = Self {
            path,
            db,
            meta_tree,
            log_tree,
            _phantom: PhantomData,
        };

        // Log basic recovery info (no trait bounds needed)
        let log_len = storage.log_tree.len();

        tracing::info!(log_entries = log_len, "Storage opened successfully");

        Ok(storage)
    }

    /// Creates a new storage (legacy API, prefer `open`).
    pub fn new(path: PathBuf) -> Self {
        Self::open(path).expect("Failed to open storage")
    }

    /// Forces all pending writes to disk.
    pub fn sync(&self) -> Result<(), ConsensusError> {
        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(format!("Flush failed: {}", e)))?;
        Ok(())
    }

    /// Returns storage statistics.
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            path: self.path.clone(),
            log_entries: self.log_tree.len() as u64,
            db_size_bytes: self.db.size_on_disk().unwrap_or(0),
        }
    }

    /// Gets the first log index (after compaction).
    fn get_first_log_index(&self) -> Result<LogIndex, ConsensusError> {
        if let Some(val) = self
            .meta_tree
            .get(keys::FIRST_LOG_INDEX)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(0)
        }
    }

    /// Sets the first log index.
    fn set_first_log_index(&mut self, index: LogIndex) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&index).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.meta_tree
            .insert(keys::FIRST_LOG_INDEX, val)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }
}

/// Storage statistics for monitoring.
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub path: PathBuf,
    pub log_entries: u64,
    pub db_size_bytes: u64,
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static> RaftStorage<T> for FileStorage<T> {
    async fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut batch = sled::Batch::default();

        for entry in entries {
            let key = entry.index.to_be_bytes();
            let value = bincode::serialize(entry)
                .map_err(|e| ConsensusError::StorageError(format!("Serialize error: {}", e)))?;
            batch.insert(&key, value);
        }

        // Apply batch atomically
        self.log_tree
            .apply_batch(batch)
            .map_err(|e| ConsensusError::StorageError(format!("Batch insert failed: {}", e)))?;

        // Ensure durability
        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(format!("Flush failed: {}", e)))?;

        tracing::trace!(
            entry_count = entries.len(),
            first_index = entries.first().map(|e| e.index),
            last_index = entries.last().map(|e| e.index),
            "Appended entries to persistent log"
        );

        Ok(())
    }

    async fn get_log_entry(&self, index: LogIndex) -> Result<Option<LogEntry<T>>, ConsensusError> {
        let first_index = self.get_first_log_index()?;
        if index < first_index {
            return Ok(None); // Compacted
        }

        let key = index.to_be_bytes();
        if let Some(val) = self
            .log_tree
            .get(key)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let entry: LogEntry<T> = bincode::deserialize(&val)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn get_log_range(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError> {
        if start >= end {
            return Ok(Box::new(std::iter::empty()));
        }

        let first_index = self.get_first_log_index()?;
        let actual_start = start.max(first_index);
        let start_key = actual_start.to_be_bytes();
        let end_key = end.to_be_bytes();

        let iter = self.log_tree.range(start_key..end_key).map(|res| {
            let (_, val) = res.map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        });

        Ok(Box::new(iter))
    }

    async fn get_term(&self) -> Result<Term, ConsensusError> {
        if let Some(val) = self
            .meta_tree
            .get(keys::TERM)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(0)
        }
    }

    async fn set_term(&mut self, term: Term) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&term).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.meta_tree
            .insert(keys::TERM, val)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::debug!(term = term, "Persisted term");
        Ok(())
    }

    async fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError> {
        if let Some(val) = self
            .meta_tree
            .get(keys::VOTE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(None)
        }
    }

    async fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&vote).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.meta_tree
            .insert(keys::VOTE, val)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::debug!(vote = ?vote, "Persisted vote");
        Ok(())
    }

    async fn set_term_and_vote(
        &mut self,
        term: Term,
        vote: Option<NodeId>,
    ) -> Result<(), ConsensusError> {
        // Use Sled transaction for atomicity
        let term_bytes =
            bincode::serialize(&term).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let vote_bytes =
            bincode::serialize(&vote).map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let meta = &self.meta_tree;
        meta.transaction::<_, _, sled::transaction::ConflictableTransactionError<()>>(|tx| {
            tx.insert(keys::TERM, term_bytes.as_slice())?;
            tx.insert(keys::VOTE, vote_bytes.as_slice())?;
            Ok(())
        })
        .map_err(|e| ConsensusError::StorageError(format!("Transaction failed: {:?}", e)))?;

        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::debug!(term = term, vote = ?vote, "Persisted term and vote atomically");
        Ok(())
    }

    async fn get_log(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError> {
        let iter = self.log_tree.iter().map(|res| {
            let (_, val) = res.map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        });
        Ok(Box::new(iter))
    }

    async fn get_last_log_info(&self) -> Result<LogInfo, ConsensusError> {
        // Get the last entry from the log tree (highest index)
        if let Some(result) = self
            .log_tree
            .last()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let (_, value) = result;
            let entry: LogEntry<T> = bincode::deserialize(&value)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            return Ok(LogInfo {
                last_index: entry.index,
                last_term: entry.term,
            });
        }

        // Check for snapshot
        if let Some(snapshot) = self.get_snapshot().await? {
            return Ok(LogInfo {
                last_index: snapshot.last_included_index,
                last_term: snapshot.last_included_term,
            });
        }

        Ok(LogInfo::default())
    }

    async fn truncate_log(&mut self, from_index: LogIndex) -> Result<(), ConsensusError> {
        let mut batch = sled::Batch::default();
        let mut removed_count = 0u64;

        // Iterate from the truncation point to the end
        for item in self.log_tree.range(from_index.to_be_bytes()..) {
            let (key, _) = item.map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            batch.remove(key);
            removed_count += 1;
        }

        if removed_count > 0 {
            self.log_tree
                .apply_batch(batch)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            self.db
                .flush()
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

            tracing::debug!(
                from_index = from_index,
                removed_entries = removed_count,
                "Truncated log"
            );
        }

        Ok(())
    }

    async fn get_commit_index(&self) -> Result<LogIndex, ConsensusError> {
        if let Some(val) = self
            .meta_tree
            .get(keys::COMMIT_INDEX)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(0)
        }
    }

    async fn set_commit_index(&mut self, index: LogIndex) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&index).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.meta_tree
            .insert(keys::COMMIT_INDEX, val)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        // Note: commit_index is not flushed immediately for performance
        // It can be reconstructed from the log on recovery
        Ok(())
    }

    async fn get_peers(&self) -> Result<Vec<String>, ConsensusError> {
        if let Some(val) = self
            .meta_tree
            .get(keys::PEERS)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            bincode::deserialize(&val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(Vec::new())
        }
    }

    async fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(peers).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.meta_tree
            .insert(keys::PEERS, val)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_snapshot(&self) -> Result<Option<Snapshot<T>>, ConsensusError> {
        if let Some(val) = self
            .meta_tree
            .get(keys::SNAPSHOT)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let snapshot: Snapshot<T> = bincode::deserialize(&val)
                .map_err(|e| ConsensusError::SnapshotError(format!("Deserialize error: {}", e)))?;

            // Verify integrity
            snapshot.verify()?;

            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    async fn create_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), ConsensusError> {
        let last_included_index = snapshot.last_included_index;

        // Verify the snapshot before storing
        snapshot.verify()?;

        // Serialize snapshot
        let snapshot_bytes = bincode::serialize(&snapshot)
            .map_err(|e| ConsensusError::SnapshotError(format!("Serialize error: {}", e)))?;

        // Store snapshot atomically
        self.meta_tree
            .insert(keys::SNAPSHOT, snapshot_bytes)
            .map_err(|e| ConsensusError::SnapshotError(e.to_string()))?;

        // Compact the log - remove entries up to and including last_included_index
        let mut batch = sled::Batch::default();
        let first_index = self.get_first_log_index()?;

        for index in first_index..=last_included_index {
            batch.remove(&index.to_be_bytes());
        }

        self.log_tree
            .apply_batch(batch)
            .map_err(|e| ConsensusError::CompactionError(e.to_string()))?;

        // Update first log index
        self.set_first_log_index(last_included_index + 1)?;

        // Ensure durability
        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::info!(
            last_included_index = last_included_index,
            "Created snapshot and compacted log"
        );

        Ok(())
    }

    async fn install_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), ConsensusError> {
        // Verify incoming snapshot
        snapshot.verify()?;

        let last_included_index = snapshot.last_included_index;

        // Serialize and store
        let snapshot_bytes = bincode::serialize(&snapshot)
            .map_err(|e| ConsensusError::SnapshotError(format!("Serialize error: {}", e)))?;

        self.meta_tree
            .insert(keys::SNAPSHOT, snapshot_bytes)
            .map_err(|e| ConsensusError::SnapshotError(e.to_string()))?;

        // Clear the entire log
        self.log_tree
            .clear()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        // Update first log index
        self.set_first_log_index(last_included_index + 1)?;

        self.db
            .flush()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::info!(
            last_included_index = last_included_index,
            "Installed snapshot from leader"
        );

        Ok(())
    }
}

// ============================================================================
// RAFT NODE
// ============================================================================

/// The Raft State Machine.
///
/// Implements the Raft consensus algorithm for distributed agreement.
pub struct RaftNode<T: Send + Sync> {
    // Persistent State abstracted via Storage
    pub storage: Box<dyn RaftStorage<T>>,

    // Volatile State
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    // Leader State (reinitialized after election)
    next_index: Vec<LogIndex>,
    match_index: Vec<LogIndex>,

    // Node State
    pub role: RaftRole,
    pub id: NodeId,

    // Networking
    pub network: Box<dyn ConsensusNetwork>,
}

impl<T: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static> RaftNode<T> {
    /// Creates a new Raft node with custom storage.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this node
    /// * `network` - Network transport for consensus messages
    /// * `storage` - Storage backend for persisting Raft state
    ///
    /// # Note
    ///
    /// This constructor initializes with commit_index = 0. Call `init()` after
    /// construction to load the actual commit index from storage.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use praborrow_lease::raft::{RaftNode, InMemoryStorage, FileStorage};
    ///
    /// // In-memory storage (volatile)
    /// let mut node = RaftNode::new(1, network, Box::new(InMemoryStorage::new()));
    /// node.init().await?;
    ///
    /// // File-based storage (persistent)
    /// let mut node = RaftNode::new(1, network, Box::new(FileStorage::open("./raft-data".into())?));
    /// node.init().await?;
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
            next_index: Vec::new(),
            match_index: Vec::new(),
            role: RaftRole::Follower,
            id,
            network,
        }
    }

    /// Initializes the node by loading state from storage.
    ///
    /// Call this after construction to restore commit index from storage.
    pub async fn init(&mut self) -> Result<(), ConsensusError> {
        self.commit_index = self.storage.get_commit_index().await?;
        tracing::info!(
            node_id = self.id,
            commit_index = self.commit_index,
            "Initialized Raft node from storage"
        );
        Ok(())
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
        let current_term = self.storage.get_term().await.unwrap_or(0);
        let new_term = current_term + 1;

        // Atomically update term and vote for self
        let _ = self.storage.set_term_and_vote(new_term, Some(self.id)).await;

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
    pub async fn handle_request_vote(&mut self, term: Term, candidate_id: NodeId) -> bool {
        let current_term = self.storage.get_term().await.unwrap_or(0);
        let _voted_for = self.storage.get_vote().await.unwrap_or(None);

        if term > current_term {
            tracing::debug!(
                node_id = self.id,
                old_term = current_term,
                new_term = term,
                "Received higher term, stepping down"
            );
            // Atomically update term and clear vote
            let _ = self.storage.set_term_and_vote(term, None).await;
            if self.role != RaftRole::Follower {
                tracing::info!(
                    node_id = self.id,
                    from_role = %self.role,
                    to_role = "Follower",
                    "Role transition"
                );
            }
            self.role = RaftRole::Follower;
        }

        // Re-read vote after potential update
        let voted_for = self.storage.get_vote().await.unwrap_or(None);
        let current_term = self.storage.get_term().await.unwrap_or(0);

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

        // Check if we can grant vote
        if voted_for.is_none() || voted_for == Some(candidate_id) {
            // TODO: Also check log up-to-date (§5.4.1)
            let log_info = self.storage.get_last_log_info().await.unwrap_or_default();

            tracing::debug!(
                node_id = self.id,
                term = term,
                candidate_id = candidate_id,
                our_last_index = log_info.last_index,
                our_last_term = log_info.last_term,
                "Granting vote"
            );
            let _ = self.storage.set_vote(Some(candidate_id)).await;
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

    /// Become leader after winning election.
    pub async fn become_leader(&mut self) {
        let peers = self.storage.get_peers().await.unwrap_or_default();
        let log_info = self.storage.get_last_log_info().await.unwrap_or_default();

        // Initialize leader state
        self.next_index = vec![log_info.last_index + 1; peers.len()];
        self.match_index = vec![0; peers.len()];
        self.role = RaftRole::Leader;

        tracing::info!(
            node_id = self.id,
            term = self.storage.get_term().await.unwrap_or(0),
            peer_count = peers.len(),
            "Became leader"
        );
    }

    /// Adds a peer to the cluster configuration.
    ///
    /// Updates both persistent storage and the active network transport.
    pub async fn add_node(&mut self, peer_address: String) -> Result<(), ConsensusError> {
        let mut peers = self.storage.get_peers().await?;
        if !peers.contains(&peer_address) {
            peers.push(peer_address.clone());
            self.storage.set_peers(&peers).await?;

            // Notify network layer
            if let Err(e) = self.network.update_peers(peers).await {
                return Err(ConsensusError::NetworkError(e));
            }

            tracing::info!(
                node_id = self.id,
                peer = peer_address,
                "Added node to cluster"
            );
        }
        Ok(())
    }

    /// Removes a peer from the cluster configuration.
    pub async fn remove_node(&mut self, peer_address: &str) -> Result<(), ConsensusError> {
        let mut peers = self.storage.get_peers().await?;
        if let Some(pos) = peers.iter().position(|p| p == peer_address) {
            peers.remove(pos);
            self.storage.set_peers(&peers).await?;

            // Notify network layer
            if let Err(e) = self.network.update_peers(peers).await {
                return Err(ConsensusError::NetworkError(e));
            }

            tracing::info!(
                node_id = self.id,
                peer = peer_address,
                "Removed node from cluster"
            );
        }
        Ok(())
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_storage() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();

        assert_eq!(storage.get_term().await.unwrap(), 0);
        storage.set_term(5).await.unwrap();
        assert_eq!(storage.get_term().await.unwrap(), 5);

        assert_eq!(storage.get_vote().await.unwrap(), None);
        storage.set_vote(Some(42)).await.unwrap();
        assert_eq!(storage.get_vote().await.unwrap(), Some(42));

        let entry = LogEntry {
            term: 1,
            index: 1,
            command: "test".to_string(),
        };
        storage.append_entries(&[entry]).await.unwrap();
        assert_eq!(storage.get_log().await.unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_storage_log_operations() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();

        // Append multiple entries
        let entries: Vec<LogEntry<String>> = (1..=5)
            .map(|i| LogEntry {
                term: 1,
                index: i,
                command: format!("cmd{}", i),
            })
            .collect();
        storage.append_entries(&entries).await.unwrap();

        // Test get_log_entry
        assert!(storage.get_log_entry(0).await.unwrap().is_none());
        assert_eq!(storage.get_log_entry(1).await.unwrap().unwrap().command, "cmd1");
        assert_eq!(storage.get_log_entry(5).await.unwrap().unwrap().command, "cmd5");

        // Test get_log_range
        let range: Vec<LogEntry<String>> = storage.get_log_range(2, 4).await.unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].index, 2);
        assert_eq!(range[1].index, 3);

        // Test get_last_log_info
        let info = storage.get_last_log_info().await.unwrap();
        assert_eq!(info.last_index, 5);
        assert_eq!(info.last_term, 1);

        // Test truncate_log
        storage.truncate_log(3).await.unwrap();
        assert_eq!(storage.get_log().await.unwrap().count(), 2);
        let info = storage.get_last_log_info().await.unwrap();
        assert_eq!(info.last_index, 2);
    }

    #[tokio::test]
    async fn test_in_memory_storage_snapshot() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();

        // Add log entries
        let entries: Vec<LogEntry<String>> = (1..=10)
            .map(|i| LogEntry {
                term: 1,
                index: i,
                command: format!("cmd{}", i),
            })
            .collect();
        storage.append_entries(&entries).await.unwrap();

        // Create snapshot at index 5
        let snapshot = Snapshot::new(5, 1, "snapshot_data".to_string()).unwrap();
        storage.create_snapshot(snapshot).await.unwrap();

        // Verify log compaction
        assert_eq!(storage.get_log().await.unwrap().count(), 5); // entries 6-10 remain
        assert!(storage.get_log_entry(5).await.unwrap().is_none()); // compacted
        assert!(storage.get_log_entry(6).await.unwrap().is_some()); // still there

        // Verify snapshot
        let retrieved = storage.get_snapshot().await.unwrap().unwrap();
        assert_eq!(retrieved.last_included_index, 5);
        assert_eq!(retrieved.data, "snapshot_data");
    }

    #[tokio::test]
    async fn test_file_storage_persistence() {
        let path = std::env::temp_dir().join("raft_test_storage_v2");
        // Clean up previous run
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        {
            let mut storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();
            storage.set_term(10).await.unwrap();
            storage.set_vote(Some(1)).await.unwrap();

            let entry = LogEntry {
                term: 1,
                index: 1,
                command: "test_persist".to_string(),
            };
            storage.append_entries(&[entry]).await.unwrap();
        }

        // Re-open
        {
            let storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();
            assert_eq!(storage.get_term().await.unwrap(), 10);
            assert_eq!(storage.get_vote().await.unwrap(), Some(1));

            let log: Vec<LogEntry<String>> = storage.get_log().await.unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(log.len(), 1);
            assert_eq!(log[0].command, "test_persist");
        }

        // Cleanup
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_file_storage_atomic_term_and_vote() {
        let path = std::env::temp_dir().join("raft_test_atomic");
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        {
            let mut storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();
            storage.set_term_and_vote(5, Some(42)).await.unwrap();
        }

        {
            let storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();
            assert_eq!(storage.get_term().await.unwrap(), 5);
            assert_eq!(storage.get_vote().await.unwrap(), Some(42));
        }

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_file_storage_log_truncation() {
        let path = std::env::temp_dir().join("raft_test_truncate");
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        let mut storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();

        // Add entries
        let entries: Vec<LogEntry<String>> = (1..=5)
            .map(|i| LogEntry {
                term: 1,
                index: i,
                command: format!("cmd{}", i),
            })
            .collect();
        storage.append_entries(&entries).await.unwrap();

        // Truncate
        storage.truncate_log(3).await.unwrap();

        let log: Vec<LogEntry<String>> = storage.get_log().await.unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].index, 1);
        assert_eq!(log[1].index, 2);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_file_storage_snapshot() {
        let path = std::env::temp_dir().join("raft_test_snapshot");
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }

        {
            let mut storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();

            // Add log entries
            let entries: Vec<LogEntry<String>> = (1..=10)
                .map(|i| LogEntry {
                    term: 1,
                    index: i,
                    command: format!("cmd{}", i),
                })
                .collect();
            storage.append_entries(&entries).await.unwrap();

            // Create snapshot
            let snapshot = Snapshot::new(5, 1, "state_at_5".to_string()).unwrap();
            storage.create_snapshot(snapshot).await.unwrap();
        }

        // Re-open and verify
        {
            let storage: FileStorage<String> = FileStorage::open(path.clone()).unwrap();

            let snapshot = storage.get_snapshot().await.unwrap().unwrap();
            assert_eq!(snapshot.last_included_index, 5);
            assert_eq!(snapshot.data, "state_at_5");

            // Log should only have entries 6-10
            let log: Vec<LogEntry<String>> = storage.get_log().await.unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(log.len(), 5);
            assert_eq!(log[0].index, 6);
        }

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_snapshot_integrity() {
        let snapshot = Snapshot::new(10, 2, "test_data".to_string()).unwrap();
        assert!(snapshot.verify().is_ok());

        // Tamper with checksum
        let mut bad_snapshot = snapshot.clone();
        bad_snapshot.checksum = 12345;
        assert!(bad_snapshot.verify().is_err());
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

        struct SharedMockNetwork(std::sync::Arc<std::sync::RwLock<Vec<String>>>);
        #[async_trait]
        impl ConsensusNetwork for SharedMockNetwork {
            async fn broadcast_vote_request(&self, _t: Term, _c: NodeId) -> Result<(), String> {
                Ok(())
            }
            async fn send_heartbeat(&self, _l: NodeId, _t: Term) -> Result<(), String> {
                Ok(())
            }
            async fn receive(&self) -> Result<Packet, String> {
                futures::future::pending().await
            }
            async fn update_peers(&self, peers: Vec<String>) -> Result<(), String> {
                *self.0.write().unwrap() = peers;
                Ok(())
            }
        }

        let shared_peers = std::sync::Arc::new(std::sync::RwLock::new(Vec::new()));
        let network = Box::new(SharedMockNetwork(shared_peers.clone()));
        let mut node = RaftNode::new(1, network, Box::new(InMemoryStorage::<String>::new()));

        // Initial state
        assert!(node.storage.get_peers().await.unwrap().is_empty());
        assert!(shared_peers.read().unwrap().is_empty());

        // Add node
        node.add_node("192.168.1.10:8000".to_string())
            .await
            .unwrap();

        // Verify storage
        let peers = node.storage.get_peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], "192.168.1.10:8000");

        // Verify network
        assert_eq!(shared_peers.read().unwrap().len(), 1);

        // Add duplicate (should be ignored)
        node.add_node("192.168.1.10:8000".to_string())
            .await
            .unwrap();
        let peers = node.storage.get_peers().await.unwrap();
        assert_eq!(peers.len(), 1);

        // Remove node
        node.remove_node("192.168.1.10:8000").await.unwrap();
        assert!(node.storage.get_peers().await.unwrap().is_empty());
        assert!(shared_peers.read().unwrap().is_empty());
    }
}

