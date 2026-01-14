use crate::builder::RaftNodeBuilder;
use crate::engine::{ConsensusError, RaftConfig};
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

// ============================================================================
// JOINT CONSENSUS - SAFE MEMBERSHIP CHANGES
// ============================================================================

/// Cluster configuration for Joint Consensus.
///
/// During a membership change, the cluster transitions through:
/// 1. `Single(old)` - Normal operation with old configuration
/// 2. `Joint(old, new)` - Transitional state requiring majorities from BOTH configs
/// 3. `Single(new)` - Normal operation with new configuration
///
/// This two-phase approach prevents split-brain scenarios during membership changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterConfig {
    /// Single configuration (normal operation)
    Single(Vec<NodeId>),
    /// Joint configuration during transition (old, new)
    Joint { old: Vec<NodeId>, new: Vec<NodeId> },
}

impl ClusterConfig {
    /// Creates a new single configuration.
    pub fn single(nodes: Vec<NodeId>) -> Self {
        Self::Single(nodes)
    }

    /// Checks if a majority is achieved.
    ///
    /// In Single mode: simple majority of the single config.
    /// In Joint mode: majority from BOTH old AND new configs.
    pub fn has_majority(&self, voters: &[NodeId]) -> bool {
        match self {
            ClusterConfig::Single(nodes) => {
                let count = voters.iter().filter(|v| nodes.contains(v)).count();
                count > nodes.len() / 2
            }
            ClusterConfig::Joint { old, new } => {
                let old_count = voters.iter().filter(|v| old.contains(v)).count();
                let new_count = voters.iter().filter(|v| new.contains(v)).count();
                old_count > old.len() / 2 && new_count > new.len() / 2
            }
        }
    }

    /// Returns all unique node IDs in the current configuration.
    pub fn all_nodes(&self) -> Vec<NodeId> {
        match self {
            ClusterConfig::Single(nodes) => nodes.clone(),
            ClusterConfig::Joint { old, new } => {
                let mut all: Vec<NodeId> = old.clone();
                for n in new {
                    if !all.contains(n) {
                        all.push(*n);
                    }
                }
                all
            }
        }
    }

    /// Checks if in joint consensus state.
    pub fn is_joint(&self) -> bool {
        matches!(self, ClusterConfig::Joint { .. })
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self::Single(vec![])
    }
}

/// Configuration change request.
///
/// Submitted to the leader to initiate a membership change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfChange {
    /// Add a node to the cluster
    AddNode(NodeId),
    /// Remove a node from the cluster
    RemoveNode(NodeId),
}

/// Internal log entry for configuration changes.
///
/// These are special log entries that, when committed, trigger
/// configuration transitions in the Raft state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfChangeEntry {
    /// Enter joint consensus with old+new config
    EnterJoint(ClusterConfig),
    /// Leave joint consensus, commit to new config
    LeaveJoint(ClusterConfig),
}

/// Command payload for a log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogCommand<T> {
    /// No-op command, often appended by a leader at the start of its term
    /// or used for heartbeats.
    NoOp,
    /// Application-level command to be applied to the state machine.
    App(T),
    /// Membership configuration change (Joint Consensus).
    Config(ClusterConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<T> {
    /// The term when this entry was created by the leader.
    pub term: Term,
    /// The index of this entry in the log.
    pub index: LogIndex,
    /// The command payload.
    pub command: LogCommand<T>,
}

impl<T> LogEntry<T> {
    /// Creates a new data entry.
    pub fn new(index: LogIndex, term: Term, command: T) -> Result<Self, ConsensusError> {
        if index == 0 {
            return Err(ConsensusError::IntegrityError(
                "Log entry index must be greater than 0".into(),
            ));
        }
        Ok(Self {
            index,
            term,
            command: LogCommand::App(command),
        })
    }

    /// Creates a new configuration entry.
    pub fn config(
        index: LogIndex,
        term: Term,
        config: ClusterConfig,
    ) -> Result<Self, ConsensusError> {
        if index == 0 {
            return Err(ConsensusError::IntegrityError(
                "Log entry index must be greater than 0".into(),
            ));
        }
        Ok(Self {
            index,
            term,
            command: LogCommand::Config(config),
        })
    }

    /// Creates a new NoOp entry.
    pub fn noop(index: LogIndex, term: Term) -> Result<Self, ConsensusError> {
        if index == 0 {
            return Err(ConsensusError::IntegrityError(
                "Log entry index must be greater than 0".into(),
            ));
        }
        Ok(Self {
            index,
            term,
            command: LogCommand::NoOp,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VersionedLogEntry<T> {
    V0(LogEntry<T>),
}

impl<T> From<LogEntry<T>> for VersionedLogEntry<T> {
    fn from(entry: LogEntry<T>) -> Self {
        VersionedLogEntry::V0(entry)
    }
}

impl<T> From<VersionedLogEntry<T>> for LogEntry<T> {
    fn from(versioned: VersionedLogEntry<T>) -> Self {
        match versioned {
            VersionedLogEntry::V0(entry) => entry,
        }
    }
}

use sha2::{Digest, Sha256};

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
    /// SHA256 checksum for integrity verification
    pub checksum: Vec<u8>,
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
        let hash = Sha256::digest(&serialized);
        let checksum = hash.to_vec();

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
        let hash = Sha256::digest(&serialized);
        let computed = hash.to_vec();

        if computed != self.checksum {
            return Err(ConsensusError::IntegrityError(format!(
                "Snapshot checksum mismatch: expected {:?}, got {:?}",
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
use std::collections::BTreeMap;

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
    log: BTreeMap<LogIndex, LogEntry<T>>,
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
            log: BTreeMap::new(),
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
        for entry in entries {
            self.log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    async fn get_log_entry(&self, index: LogIndex) -> Result<Option<LogEntry<T>>, ConsensusError> {
        Ok(self.log.get(&index).cloned())
    }

    async fn get_log_range(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError>
    {
        if start >= end {
            return Ok(Box::new(std::iter::empty()));
        }
        let entries: Vec<Result<LogEntry<T>, ConsensusError>> = self
            .log
            .range(start..end)
            .map(|(_, e)| Ok(e.clone()))
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
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError>
    {
        let entries: Vec<Result<LogEntry<T>, ConsensusError>> =
            self.log.values().cloned().map(Ok).collect();
        Ok(Box::new(entries.into_iter()))
    }

    async fn get_last_log_info(&self) -> Result<LogInfo, ConsensusError> {
        if let Some((_, entry)) = self.log.last_key_value() {
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
        // Removes all entries with index >= from_index
        let _removed = self.log.split_off(&from_index);
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
        // split_off returns entries >= (compact_up_to + 1), which we want to KEEP
        self.log = self.log.split_off(&(compact_up_to + 1));
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

// ============================================================================
// PRODUCTION FILE-BASED STORAGE (REDB)
// ============================================================================

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::sync::Arc;

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

// Metadata keys
const KEY_TERM: &str = "term";
const KEY_VOTE: &str = "vote";
const KEY_COMMIT_INDEX: &str = "commit_index";
const KEY_PEERS: &str = "peers";
const KEY_SNAPSHOT: &str = "snapshot";
const KEY_FIRST_LOG_INDEX: &str = "first_log_index";

/// Production-grade file-based storage for Raft using `redb`.
///
/// Provides ACID guarantees using persistent transactions.
pub struct FileStorage<T> {
    /// Path to the storage file (for stats/logging)
    path: PathBuf,
    /// Redb database instance
    db: Database,
    /// Metrics for observability
    metrics: Option<Arc<crate::metrics::RaftMetrics>>,
    /// Phantom data for T
    _phantom: PhantomData<T>,
}

impl<T> FileStorage<T> {
    /// Opens or creates a persistent Raft storage at the given path.
    pub fn open(
        path: PathBuf,
        metrics: Option<Arc<crate::metrics::RaftMetrics>>,
    ) -> Result<Self, ConsensusError> {
        tracing::info!(
            path = %path.display(),
            "Opening persistent Raft storage (redb)"
        );

        let db = Database::create(&path)
            .map_err(|e| ConsensusError::StorageError(format!("Failed to open redb: {}", e)))?;

        // Initialize tables if needed
        let txn = db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let _ = txn
                .open_table(LOG_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            let _ = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let storage = Self {
            path,
            db,
            metrics,
            _phantom: PhantomData,
        };

        // Log recovery info
        let read_txn = storage
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let log_table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        tracing::info!(
            log_entries = log_table.len().unwrap_or(0),
            "Storage opened successfully"
        );

        Ok(storage)
    }

    /// Creates a new storage (legacy API, prefer `open`).
    pub fn new(path: PathBuf) -> Self {
        Self::open(path, None).expect("Failed to open storage")
    }

    /// Forces all pending writes to disk. (Redb is durable on commit, this is mostly explicit).
    pub fn sync(&self) -> Result<(), ConsensusError> {
        // Redb commits are durable. We can ensure the db checkpointer runs if needed,
        // but generally txn.commit() is enough.
        Ok(())
    }

    /// Returns storage statistics.
    pub fn stats(&self) -> StorageStats {
        // Estimate size from file
        let db_size = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);

        let log_entries = if let Ok(txn) = self.db.begin_read() {
            if let Ok(table) = txn.open_table(LOG_TABLE) {
                table.len().unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };

        StorageStats {
            path: self.path.clone(),
            log_entries,
            db_size_bytes: db_size,
        }
    }

    /// Gets the first log index (after compaction).
    fn get_first_log_index(&self) -> Result<LogIndex, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(META_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(KEY_FIRST_LOG_INDEX)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            bincode::deserialize(val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(0)
        }
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
impl<T: Clone + Send + Sync + Serialize + serde::de::DeserializeOwned + 'static> RaftStorage<T>
    for FileStorage<T>
{
    async fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), ConsensusError> {
        if entries.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        {
            let mut table = txn
                .open_table(LOG_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

            for entry in entries {
                let versioned: VersionedLogEntry<T> = entry.clone().into();
                let value = bincode::serialize(&versioned)
                    .map_err(|e| ConsensusError::StorageError(format!("Serialize error: {}", e)))?;
                table
                    .insert(entry.index, value.as_slice())
                    .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            }
        }

        txn.commit()
            .map_err(|e| ConsensusError::StorageError(format!("Commit failed: {}", e)))?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_disk_write(start.elapsed());
        }

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

        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(LOG_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(index)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            let versioned: VersionedLogEntry<T> = bincode::deserialize(val)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            let entry: LogEntry<T> = versioned.into();
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn get_log_range(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError>
    {
        if start >= end {
            return Ok(Box::new(std::iter::empty()));
        }

        let first_index = self.get_first_log_index()?;
        let actual_start = start.max(first_index);

        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(LOG_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let mut entries = Vec::new();
        for item in table
            .range(actual_start..end)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let (_, val) = item.map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            let val = val.value();
            let versioned: VersionedLogEntry<T> = bincode::deserialize(val)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            entries.push(Ok(versioned.into()));
        }

        Ok(Box::new(entries.into_iter()))
    }

    async fn get_term(&self) -> Result<Term, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(META_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(KEY_TERM)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            bincode::deserialize(val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(0)
        }
    }

    async fn set_term(&mut self, term: Term) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&term).map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let mut table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            table
                .insert(KEY_TERM, val.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::debug!(term = term, "Persisted term");
        Ok(())
    }

    async fn get_vote(&self) -> Result<Option<NodeId>, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(META_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(KEY_VOTE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            bincode::deserialize(val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(None)
        }
    }

    async fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&vote).map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let mut table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            table
                .insert(KEY_VOTE, val.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::debug!(vote = ?vote, "Persisted vote");
        Ok(())
    }

    async fn set_term_and_vote(
        &mut self,
        term: Term,
        vote: Option<NodeId>,
    ) -> Result<(), ConsensusError> {
        let term_bytes =
            bincode::serialize(&term).map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let vote_bytes =
            bincode::serialize(&vote).map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let mut table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            table
                .insert(KEY_TERM, term_bytes.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            table
                .insert(KEY_VOTE, vote_bytes.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::debug!(term = term, vote = ?vote, "Persisted term and vote atomically");
        Ok(())
    }

    async fn get_log(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<LogEntry<T>, ConsensusError>> + Send>, ConsensusError>
    {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(LOG_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let mut entries = Vec::new();
        // iter() on table returns all entries
        for item in table
            .iter()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let (_, val) = item.map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            let val = val.value();
            let versioned: VersionedLogEntry<T> = bincode::deserialize(val)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            entries.push(Ok(versioned.into()));
        }

        Ok(Box::new(entries.into_iter()))
    }

    async fn get_last_log_info(&self) -> Result<LogInfo, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(LOG_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some((_, val)) = table
            .last()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            let versioned: VersionedLogEntry<T> = bincode::deserialize(val)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            let entry: LogEntry<T> = versioned.into();
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
        // Redb doesn't support range delete directly in one call, need to iterate keys to delete?
        // Wait, `redb` doesn't support batch remove by range efficiently?
        // We have to iterate and delete.

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let count_removed = {
            let mut table = txn
                .open_table(LOG_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            // We need to collect keys first to avoid concurrent modification issues if that's a thing,
            // but redb might allow it. Safest is collect keys.
            // Actually `table.range(from_index..)` returns an iterator.
            // We can't iterate and delete on same table access usually.
            // Let's collect keys.
            let keys: Vec<u64> = table
                .range(from_index..)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?
                .map(|r| r.unwrap().0.value())
                .collect();

            let count = keys.len();
            for key in keys {
                table
                    .remove(key)
                    .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            }
            count
        };

        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if count_removed > 0 {
            tracing::debug!(
                from_index = from_index,
                removed_entries = count_removed,
                "Truncated log"
            );
        }

        Ok(())
    }

    async fn get_commit_index(&self) -> Result<LogIndex, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(META_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(KEY_COMMIT_INDEX)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            bincode::deserialize(val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(0)
        }
    }

    async fn set_commit_index(&mut self, index: LogIndex) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(&index).map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let mut table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            table
                .insert(KEY_COMMIT_INDEX, val.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_peers(&self) -> Result<Vec<String>, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(META_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(KEY_PEERS)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            bincode::deserialize(val).map_err(|e| ConsensusError::StorageError(e.to_string()))
        } else {
            Ok(Vec::new())
        }
    }

    async fn set_peers(&mut self, peers: &[String]) -> Result<(), ConsensusError> {
        let val =
            bincode::serialize(peers).map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let mut table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            table
                .insert(KEY_PEERS, val.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_snapshot(&self) -> Result<Option<Snapshot<T>>, ConsensusError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        let table = txn
            .open_table(META_TABLE)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        if let Some(val) = table
            .get(KEY_SNAPSHOT)
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?
        {
            let val = val.value();
            let snapshot: Snapshot<T> = bincode::deserialize(val)
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

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            // Store snapshot
            let mut meta_table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            meta_table
                .insert(KEY_SNAPSHOT, snapshot_bytes.as_slice())
                .map_err(|e| ConsensusError::SnapshotError(e.to_string()))?;

            // Compact log
            // Remove entries <= last_included_index
            let mut log_table = txn
                .open_table(LOG_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            // Same issue: iterate and delete.
            // We need entries <= last_included_index.
            // redb range is inclusive/exclusive? RangeBound.
            // We can range(..=last_included_index)
            let keys: Vec<u64> = log_table
                .range(..=last_included_index)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?
                .map(|r| r.unwrap().0.value())
                .collect();

            for key in keys {
                log_table
                    .remove(key)
                    .map_err(|e| ConsensusError::CompactionError(e.to_string()))?;
            }

            // Update first log index
            // Store first_log_index = last_included_index + 1
            let first_index_val = bincode::serialize(&(last_included_index + 1))
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            meta_table
                .insert(KEY_FIRST_LOG_INDEX, first_index_val.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }

        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::info!(
            last_included_index = last_included_index,
            "Created snapshot and compacted log (redb)"
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

        let txn = self
            .db
            .begin_write()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        {
            let mut meta_table = txn
                .open_table(META_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            meta_table
                .insert(KEY_SNAPSHOT, snapshot_bytes.as_slice())
                .map_err(|e| ConsensusError::SnapshotError(e.to_string()))?;

            // Clear entire log?
            // "Clear the entire log" in old impl.
            // redb doesn't have clear(). Iter keys and delete.
            let mut log_table = txn
                .open_table(LOG_TABLE)
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

            // Iterate all
            let keys: Vec<u64> = log_table
                .iter()
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?
                .map(|r| r.unwrap().0.value())
                .collect();
            for key in keys {
                log_table
                    .remove(key)
                    .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            }

            // Update first log index
            let first_index_val = bincode::serialize(&(last_included_index + 1))
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
            meta_table
                .insert(KEY_FIRST_LOG_INDEX, first_index_val.as_slice())
                .map_err(|e| ConsensusError::StorageError(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| ConsensusError::StorageError(e.to_string()))?;

        tracing::info!(
            last_included_index = last_included_index,
            "Installed snapshot from leader (redb)"
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

    // Configuration
    pub config: RaftConfig,
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
        config: RaftConfig,
    ) -> Self {
        tracing::info!(node_id = id, "Creating new Raft node");

        Self {
            storage,
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            role: RaftRole::Follower,
            id,
            network,
            config,
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
        Self::new(
            id,
            network,
            Box::new(InMemoryStorage::new()),
            RaftConfig::default(),
        )
    }

    /// Returns a new fluent builder for constructing a RaftNode.
    pub fn builder() -> RaftNodeBuilder<T> {
        RaftNodeBuilder::new()
    }

    /// Transition to Candidate and start election.
    pub async fn start_election(&mut self) {
        let current_term = self.storage.get_term().await.unwrap_or(0);
        let new_term = current_term + 1;

        // Atomically update term and vote for self
        let _ = self
            .storage
            .set_term_and_vote(new_term, Some(self.id))
            .await;

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
    pub async fn handle_request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> bool {
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
            let log_info = self.storage.get_last_log_info().await.unwrap_or_default();

            // Raft §5.4.1: Election Restriction
            if !self.is_log_up_to_date(last_log_index, last_log_term, &log_info) {
                tracing::debug!(
                    node_id = self.id,
                    term = term,
                    candidate_id = candidate_id,
                    our_last_index = log_info.last_index,
                    our_last_term = log_info.last_term,
                    candidate_last_index = last_log_index,
                    candidate_last_term = last_log_term,
                    "Rejecting vote: candidate log not up-to-date"
                );
                return false;
            }

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

    /// Checks if candidate's log is at least as up-to-date as ours (§5.4.1)
    fn is_log_up_to_date(
        &self,
        last_log_index: LogIndex,
        last_log_term: Term,
        our_log: &LogInfo,
    ) -> bool {
        // Candidate's log is more up-to-date if:
        // 1. Its last term is greater, OR
        // 2. Same term but longer log
        if last_log_term > our_log.last_term {
            return true;
        }
        if last_log_term == our_log.last_term && last_log_index >= our_log.last_index {
            return true;
        }
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

    /// Adds a peer to the cluster configuration (LEGACY - use propose_conf_change for safety).
    ///
    /// ⚠️ WARNING: This method is NOT safe for concurrent membership changes.
    /// Use `propose_conf_change` for production deployments.
    #[deprecated(
        since = "0.7.2",
        note = "Use propose_conf_change for Joint Consensus safety"
    )]
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

    /// Removes a peer from the cluster configuration (LEGACY - use propose_conf_change for safety).
    #[deprecated(
        since = "0.7.2",
        note = "Use propose_conf_change for Joint Consensus safety"
    )]
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

    /// Proposes a configuration change using Joint Consensus (two-phase).
    ///
    /// This is the SAFE way to add/remove nodes from a running cluster.
    /// The change follows the Raft Joint Consensus protocol:
    ///
    /// 1. **Phase 1 (Enter Joint)**: Append a log entry with `C_old,new` configuration.
    ///    Requires majorities from BOTH old and new configs to commit.
    ///
    /// 2. **Phase 2 (Leave Joint)**: After `C_old,new` is committed, append a log entry
    ///    with `C_new` configuration. This finalizes the transition.
    ///
    /// # Arguments
    ///
    /// * `change` - The configuration change to propose (AddNode or RemoveNode).
    /// * `current_config` - The current cluster configuration.
    ///
    /// # Returns
    ///
    /// The new `ClusterConfig` after initiating the first phase (Joint config).
    /// The caller must track commit progress and call `finalize_conf_change` after commit.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - This node is not the leader.
    /// - A configuration change is already in progress (config is Joint).
    pub async fn propose_conf_change(
        &mut self,
        change: ConfChange,
        current_config: ClusterConfig,
    ) -> Result<ClusterConfig, ConsensusError> {
        // TODO: Implement Automated Joint Consensus
        // Current implementation is manual (Client drives Phase 1 -> Phase 2).
        // Future goal: Leader automatically schedules Phase 2 after Phase 1 commit.
        // Only leader can propose changes
        if self.role != RaftRole::Leader {
            return Err(ConsensusError::NotLeader);
        }

        // Cannot start new change if already in joint state
        if current_config.is_joint() {
            return Err(ConsensusError::ConfigChangeInProgress);
        }

        let old_nodes = match &current_config {
            ClusterConfig::Single(nodes) => nodes.clone(),
            ClusterConfig::Joint { .. } => unreachable!(), // Checked above
        };

        // Compute new configuration
        let new_nodes = match &change {
            ConfChange::AddNode(node_id) => {
                let mut nodes = old_nodes.clone();
                if !nodes.contains(node_id) {
                    nodes.push(*node_id);
                }
                nodes
            }
            ConfChange::RemoveNode(node_id) => old_nodes
                .iter()
                .filter(|n| *n != node_id)
                .copied()
                .collect(),
        };

        let joint_config = ClusterConfig::Joint {
            old: old_nodes.clone(),
            new: new_nodes.clone(),
        };

        tracing::info!(
            node_id = self.id,
            change = ?change,
            old_config_size = old_nodes.len(),
            new_config_size = new_nodes.len(),
            "Entering Joint Consensus"
        );

        Ok(joint_config)
    }

    /// Finalizes a configuration change by transitioning from Joint to Single config.
    ///
    /// Call this AFTER the Joint config entry has been committed (replicated to majority
    /// of BOTH old and new configs).
    ///
    /// # Arguments
    ///
    /// * `joint_config` - The current Joint configuration.
    ///
    /// # Returns
    ///
    /// The new Single configuration (the "new" part of the Joint).
    pub async fn finalize_conf_change(
        &mut self,
        joint_config: ClusterConfig,
    ) -> Result<ClusterConfig, ConsensusError> {
        match joint_config {
            ClusterConfig::Joint { old: _, new } => {
                let final_config = ClusterConfig::Single(new.clone());

                tracing::info!(
                    node_id = self.id,
                    new_config_size = new.len(),
                    "Leaving Joint Consensus, finalizing new configuration"
                );

                Ok(final_config)
            }
            ClusterConfig::Single(_) => Err(ConsensusError::ConfigChangeError(
                "Cannot finalize: not in Joint state".to_string(),
            )),
        }
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

        let entry = LogEntry::new(1, 1, "test".to_string()).unwrap();
        storage.append_entries(&[entry]).await.unwrap();
        assert_eq!(storage.get_log().await.unwrap().count(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_storage_log_operations() {
        let mut storage: InMemoryStorage<String> = InMemoryStorage::new();

        // Append multiple entries
        let entries: Vec<LogEntry<String>> = (1..=5)
            .map(|i| LogEntry::new(i, 1, format!("cmd{}", i)).unwrap())
            .collect();
        storage.append_entries(&entries).await.unwrap();

        // Test get_log_entry
        assert!(storage.get_log_entry(0).await.unwrap().is_none());
        let entry1 = storage.get_log_entry(1).await.unwrap().unwrap();
        if let LogCommand::App(cmd) = entry1.command {
            assert_eq!(cmd, "cmd1");
        }
        let entry5 = storage.get_log_entry(5).await.unwrap().unwrap();
        if let LogCommand::App(cmd) = entry5.command {
            assert_eq!(cmd, "cmd5");
        }

        // Test get_log_range
        let range: Vec<LogEntry<String>> = storage
            .get_log_range(2, 4)
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
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
            .map(|i| LogEntry::new(i, 1, format!("cmd{}", i)).unwrap())
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
        let path = std::env::temp_dir().join("raft_test_storage_v2.redb");
        // Clean up previous run
        if path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
        }

        {
            let mut storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();
            storage.set_term(10).await.unwrap();
            storage.set_vote(Some(1)).await.unwrap();

            let entry = LogEntry::new(1, 1, "test_persist".to_string()).unwrap();
            storage.append_entries(&[entry]).await.unwrap();
        }

        // Re-open
        {
            let storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();
            assert_eq!(storage.get_term().await.unwrap(), 10);
            assert_eq!(storage.get_vote().await.unwrap(), Some(1));

            let log: Vec<LogEntry<String>> = storage
                .get_log()
                .await
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(log.len(), 1);
            if let LogCommand::App(cmd) = &log[0].command {
                assert_eq!(cmd, "test_persist");
            }
        }

        // Cleanup
        // Cleanup
        if path.is_dir() {
            std::fs::remove_dir_all(path).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_file_storage_atomic_term_and_vote() {
        let path = std::env::temp_dir().join("raft_test_atomic.redb");
        if path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
        }

        {
            let mut storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();
            storage.set_term_and_vote(5, Some(42)).await.unwrap();
        }

        {
            let storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();
            assert_eq!(storage.get_term().await.unwrap(), 5);
            assert_eq!(storage.get_vote().await.unwrap(), Some(42));
        }

        if path.is_dir() {
            std::fs::remove_dir_all(path).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_file_storage_log_truncation() {
        let path = std::env::temp_dir().join("raft_test_truncate.redb");
        if path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
        }

        let mut storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();

        // Add entries
        let entries: Vec<LogEntry<String>> = (1..=5)
            .map(|i| LogEntry::new(i, 1, format!("cmd{}", i)).unwrap())
            .collect();
        storage.append_entries(&entries).await.unwrap();

        // Truncate
        storage.truncate_log(3).await.unwrap();

        let log: Vec<LogEntry<String>> = storage
            .get_log()
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].index, 1);
        assert_eq!(log[1].index, 2);

        if path.is_dir() {
            std::fs::remove_dir_all(path).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_file_storage_snapshot() {
        let path = std::env::temp_dir().join("raft_test_snapshot.redb");
        if path.exists() {
            if path.is_dir() {
                std::fs::remove_dir_all(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
        }

        {
            let mut storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();

            // Add log entries
            let entries: Vec<LogEntry<String>> = (1..=10)
                .map(|i| LogEntry::new(i, 1, format!("cmd{}", i)).unwrap())
                .collect();
            storage.append_entries(&entries).await.unwrap();

            // Create snapshot
            let snapshot = Snapshot::new(5, 1, "state_at_5".to_string()).unwrap();
            storage.create_snapshot(snapshot).await.unwrap();
        }

        // Re-open and verify
        {
            let storage: FileStorage<String> = FileStorage::open(path.clone(), None).unwrap();

            let snapshot = storage.get_snapshot().await.unwrap().unwrap();
            assert_eq!(snapshot.last_included_index, 5);
            assert_eq!(snapshot.data, "state_at_5");

            // Log should only have entries 6-10
            let log: Vec<LogEntry<String>> = storage
                .get_log()
                .await
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(log.len(), 5);
            assert_eq!(log[0].index, 6);
        }

        if path.is_dir() {
            std::fs::remove_dir_all(path).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn test_snapshot_integrity() {
        let snapshot = Snapshot::new(10, 2, "test_data".to_string()).unwrap();
        assert!(snapshot.verify().is_ok());

        // Tamper with checksum
        let mut bad_snapshot = snapshot.clone();
        bad_snapshot.checksum = vec![1, 2, 3, 4];
        assert!(bad_snapshot.verify().is_err());
    }

    #[test]
    fn test_raft_role_display() {
        assert_eq!(RaftRole::Follower.to_string(), "Follower");
        assert_eq!(RaftRole::Candidate.to_string(), "Candidate");
        assert_eq!(RaftRole::Leader.to_string(), "Leader");
    }

    #[tokio::test]
    #[allow(deprecated)]
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
        let mut node = RaftNode::new(
            1,
            network,
            Box::new(InMemoryStorage::<String>::new()),
            RaftConfig::default(),
        );

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
