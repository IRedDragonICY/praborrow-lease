//! Consensus Engine Implementation
//!
//! Provides the full Raft consensus loop with leader election, log replication,
//! and commit index advancement.

use crate::network::{ConsensusNetwork, NetworkError, RaftMessage, RaftNetwork};
use crate::raft::{
    ClusterConfig, LogCommand, LogEntry, LogIndex, LogInfo, NodeId, RaftRole, RaftStorage, Term,
};
use async_trait::async_trait;
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;

use std::time::Duration;

// ============================================================================
// CONFIGURATION
// ============================================================================

use serde::Deserialize;

/// Raft timing configuration
#[derive(Debug, Clone, Deserialize)]
pub struct RaftConfig {
    /// Minimum election timeout (randomized between min and max)
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_election_min"
    )]
    pub election_timeout_min: Duration,
    /// Maximum election timeout
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_election_max"
    )]
    pub election_timeout_max: Duration,
    /// Heartbeat interval (must be << election timeout)
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_heartbeat"
    )]
    pub heartbeat_interval: Duration,
    /// RPC timeout
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_rpc_timeout"
    )]
    pub rpc_timeout: Duration,
    /// Max entries per AppendEntries RPC
    #[serde(default = "default_max_entries")]
    pub max_entries_per_rpc: usize,
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let ms = u64::deserialize(deserializer)?;
    Ok(Duration::from_millis(ms))
}

fn default_election_min() -> Duration {
    Duration::from_millis(150)
}
fn default_election_max() -> Duration {
    Duration::from_millis(300)
}
fn default_heartbeat() -> Duration {
    Duration::from_millis(50)
}
fn default_rpc_timeout() -> Duration {
    Duration::from_millis(100)
}
fn default_max_entries() -> usize {
    100
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: default_election_min(),
            election_timeout_max: default_election_max(),
            heartbeat_interval: default_heartbeat(),
            rpc_timeout: default_rpc_timeout(),
            max_entries_per_rpc: default_max_entries(),
        }
    }
}

impl RaftConfig {
    /// Loads configuration from `raft.toml` and environment variables.
    ///
    /// # Priority (Highest first):
    /// 1. Environment variables (PRABORROW_*)
    /// 2. `raft.toml` file
    /// 3. Defaults
    pub fn load() -> Result<Self, config::ConfigError> {
        let builder = config::Config::builder()
            .add_source(config::File::with_name("raft").required(false))
            .add_source(config::Environment::with_prefix("PRABORROW"));

        builder.build()?.try_deserialize()
    }

    /// Returns a randomized election timeout
    pub fn random_election_timeout(&self) -> Duration {
        let min = self.election_timeout_min.as_millis() as u64;
        let max = self.election_timeout_max.as_millis() as u64;
        let timeout_ms = rand::rng().random_range(min..=max);
        Duration::from_millis(timeout_ms)
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err("election_timeout_min must be less than election_timeout_max".to_string());
        }
        // Strict safety check to prevent infinite election loops
        if self.heartbeat_interval.mul_f64(2.0) > self.election_timeout_min {
            return Err(
                "heartbeat_interval must be at most half of election_timeout_min".to_string(),
            );
        }
        if self.heartbeat_interval >= self.election_timeout_min {
            return Err("heartbeat_interval must be less than election_timeout_min".to_string());
        }
        if self.rpc_timeout.is_zero() {
            return Err("rpc_timeout must be non-zero".to_string());
        }
        if self.max_entries_per_rpc == 0 {
            return Err("max_entries_per_rpc must be positive".to_string());
        }
        Ok(())
    }

    /// Returns a new builder for configuration.
    pub fn builder() -> RaftConfigBuilder {
        RaftConfigBuilder::default()
    }
}

/// Builder for RaftConfig.
#[derive(Default)]
pub struct RaftConfigBuilder {
    election_timeout_min: Option<Duration>,
    election_timeout_max: Option<Duration>,
    heartbeat_interval: Option<Duration>,
    rpc_timeout: Option<Duration>,
    max_entries_per_rpc: Option<usize>,
}

impl RaftConfigBuilder {
    pub fn election_timeout_min(mut self, timeout: Duration) -> Self {
        self.election_timeout_min = Some(timeout);
        self
    }

    pub fn election_timeout_max(mut self, timeout: Duration) -> Self {
        self.election_timeout_max = Some(timeout);
        self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = Some(interval);
        self
    }

    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    pub fn max_entries_per_rpc(mut self, max: usize) -> Self {
        self.max_entries_per_rpc = Some(max);
        self
    }

    pub fn build(self) -> Result<RaftConfig, String> {
        let config = RaftConfig {
            election_timeout_min: self
                .election_timeout_min
                .unwrap_or_else(default_election_min),
            election_timeout_max: self
                .election_timeout_max
                .unwrap_or_else(default_election_max),
            heartbeat_interval: self.heartbeat_interval.unwrap_or_else(default_heartbeat),
            rpc_timeout: self.rpc_timeout.unwrap_or_else(default_rpc_timeout),
            max_entries_per_rpc: self.max_entries_per_rpc.unwrap_or_else(default_max_entries),
        };
        config.validate()?;
        Ok(config)
    }
}

/// Types of consensus algorithms supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusStrategy {
    /// Raft consensus algorithm
    Raft,
    /// Paxos consensus algorithm (future work)
    Paxos,
}

// ============================================================================
// ERRORS
// ============================================================================

/// Error type for consensus engine operations.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ConsensusError {
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
    #[error("Snapshot error: {0}")]
    SnapshotError(String),
    #[error("Compaction error: {0}")]
    CompactionError(String),
    #[error("Integrity check failed: {0}")]
    IntegrityError(String),
    #[error("Index out of bounds: requested {requested}, available {available}")]
    IndexOutOfBounds { requested: u64, available: u64 },
    #[error("Configuration change error: {0}")]
    ConfigChangeError(String),
    #[error("Configuration change in progress")]
    ConfigChangeInProgress,
    #[cfg(feature = "grpc")]
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("Shutdown requested")]
    Shutdown,
}

impl From<NetworkError> for ConsensusError {
    fn from(e: NetworkError) -> Self {
        ConsensusError::NetworkError(e.to_string())
    }
}

impl From<Box<dyn std::error::Error>> for ConsensusError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        ConsensusError::NetworkError(e.to_string())
    }
}

#[cfg(feature = "grpc")]
impl From<tonic::transport::Error> for ConsensusError {
    fn from(e: tonic::transport::Error) -> Self {
        ConsensusError::Tls(e.to_string())
    }
}

// ============================================================================
// CONSENSUS ENGINE TRAIT
// ============================================================================

/// Abstract interface for a consensus engine.
#[async_trait]
pub trait ConsensusEngine<T>: Send {
    /// Starts the consensus loop.
    async fn run(&mut self) -> Result<(), ConsensusError>;

    /// Proposes a new value to be agreed upon.
    async fn propose(&mut self, value: T) -> Result<LogIndex, ConsensusError>;

    /// Proposes a configuration change (membership change).
    async fn propose_conf_change(
        &mut self,
        change: crate::raft::ConfChange,
    ) -> Result<LogIndex, ConsensusError>;

    /// Returns the current leader ID, if known.
    fn leader_id(&self) -> Option<NodeId>;

    /// Returns true if this node is the leader.
    fn is_leader(&self) -> bool;

    /// Returns the current term.
    async fn current_term(&self) -> Term;

    /// Returns the commit index.
    fn commit_index(&self) -> LogIndex;
}

// ============================================================================
// RAFT CONSENSUS ENGINE
// ============================================================================

/// Leader-specific volatile state
#[derive(Debug, Default)]
struct LeaderState {
    /// For each peer: index of next log entry to send
    next_index: HashMap<NodeId, LogIndex>,
    /// For each peer: highest log entry known to be replicated
    match_index: HashMap<NodeId, LogIndex>,
}

impl LeaderState {
    fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for &peer in peers {
            next_index.insert(peer, last_log_index + 1);
            match_index.insert(peer, 0);
        }

        Self {
            next_index,
            match_index,
        }
    }
}

/// Full Raft consensus engine implementation.
pub struct RaftEngine<T, N, S>
where
    T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    N: RaftNetwork<T>,
    S: RaftStorage<T>,
{
    // Node identity
    id: NodeId,

    // Persistent state (via storage)
    storage: S,

    // Volatile state
    role: RaftRole,
    commit_index: LogIndex,
    _last_applied: LogIndex,

    // Leader state (None if not leader)
    leader_state: Option<LeaderState>,

    // Known leader (for redirecting clients)
    current_leader: Option<NodeId>,

    // Network transport
    network: N,

    // Configuration
    config: RaftConfig,

    /// Current cluster configuration (Joint Consensus supported)
    cluster_config: ClusterConfig,

    // Votes received in current election
    votes_received: HashMap<NodeId, bool>,

    // Phantom
    _phantom: std::marker::PhantomData<T>,
}

impl<T, N, S> RaftEngine<T, N, S>
where
    T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    N: RaftNetwork<T>,
    S: RaftStorage<T>,
{
    /// Creates a new Raft engine.
    ///
    /// Note: This constructor initializes with default values. Call `init()` after
    /// construction to load state from storage.
    pub fn new(id: NodeId, network: N, storage: S, config: RaftConfig) -> Self {
        tracing::info!(node_id = id, "Creating Raft engine");

        let peers = network.peer_ids();
        let mut nodes = vec![id];
        nodes.extend(peers);

        Self {
            id,
            storage,
            role: RaftRole::Follower,
            commit_index: 0,
            _last_applied: 0,
            leader_state: None,
            current_leader: None,
            network,
            config,
            votes_received: HashMap::new(),
            cluster_config: ClusterConfig::Single(nodes),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns peer IDs from network
    fn peer_ids(&self) -> Vec<NodeId> {
        self.network.peer_ids()
    }

    /// Checks if a majority is achieved based on the current cluster configuration.
    fn has_majority(&self, votes: &HashMap<NodeId, bool>) -> bool {
        let voters: Vec<NodeId> = votes
            .iter()
            .filter(|&(_, &granted)| granted)
            .map(|(&id, _)| id)
            .collect();

        self.cluster_config.has_majority(&voters)
    }

    // ========================================================================
    // ROLE TRANSITIONS
    // ========================================================================

    async fn become_follower(&mut self, term: Term) {
        let old_role = self.role.clone();
        self.role = RaftRole::Follower;
        self.leader_state = None;
        self.votes_received.clear();

        let _ = self.storage.set_term(term).await;
        let _ = self.storage.set_vote(None).await;

        if old_role != RaftRole::Follower {
            tracing::info!(
                node_id = self.id,
                from_role = %old_role,
                new_term = term,
                "Became follower"
            );
        }
    }

    async fn become_candidate(&mut self) {
        let current_term = self.storage.get_term().await.unwrap_or(0);
        let new_term = current_term + 1;

        self.role = RaftRole::Candidate;
        self.leader_state = None;
        self.current_leader = None;
        self.votes_received.clear();

        // Vote for self
        let _ = self
            .storage
            .set_term_and_vote(new_term, Some(self.id))
            .await;
        self.votes_received.insert(self.id, true);

        tracing::info!(
            node_id = self.id,
            term = new_term,
            "Became candidate, starting election"
        );
    }

    async fn become_leader(&mut self) {
        let term = self.storage.get_term().await.unwrap_or(0);
        let log_info = self.storage.get_last_log_info().await.unwrap_or_default();

        self.role = RaftRole::Leader;
        self.current_leader = Some(self.id);
        self.leader_state = Some(LeaderState::new(&self.peer_ids(), log_info.last_index));
        self.votes_received.clear();

        tracing::info!(node_id = self.id, term = term, "Became leader");
    }

    // ========================================================================
    // MAIN CONSENSUS LOOP
    // ========================================================================

    /// Runs the main consensus loop.
    pub async fn run_loop(&mut self) -> Result<(), ConsensusError> {
        tracing::info!(node_id = self.id, "Starting Raft consensus loop");

        loop {
            match &self.role {
                RaftRole::Follower => self.run_follower().await?,
                RaftRole::Candidate => self.run_candidate().await?,
                RaftRole::Leader => self.run_leader().await?,
            }
        }
    }

    /// Follower loop: wait for heartbeats, trigger election on timeout
    async fn run_follower(&mut self) -> Result<(), ConsensusError> {
        let timeout = self.config.random_election_timeout();

        tokio::select! {
            // Wait for message
            result = self.network.receive() => {
                match result {
                    Ok(msg) => self.handle_message(msg).await?,
                    Err(e) => {
                        tracing::warn!(error = %e, "Network receive error");
                    }
                }
            }

            // Election timeout
            _ = tokio::time::sleep(timeout) => {
                tracing::debug!(
                    node_id = self.id,
                    timeout_ms = timeout.as_millis(),
                    "Election timeout, becoming candidate"
                );
                self.become_candidate().await;
            }
        }

        Ok(())
    }

    /// Candidate loop: request votes, collect responses
    async fn run_candidate(&mut self) -> Result<(), ConsensusError> {
        let term = self.storage.get_term().await.unwrap_or(0);
        let log_info = self.storage.get_last_log_info().await.unwrap_or_default();

        // Send RequestVote to all peers
        for peer_id in self.peer_ids() {
            let _ = self
                .network
                .send_request_vote(
                    peer_id,
                    term,
                    self.id,
                    log_info.last_index,
                    log_info.last_term,
                )
                .await;
        }

        let timeout = self.config.random_election_timeout();
        let deadline = tokio::time::Instant::now() + timeout;

        // Collect votes until timeout or majority
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline - tokio::time::Instant::now();

            tokio::select! {
                result = self.network.receive() => {
                    match result {
                        Ok(msg) => {
                            self.handle_message(msg).await?;

                            // Check if we won
                            if self.role == RaftRole::Leader {
                                return Ok(());
                            }

                            // Check if we lost (became follower)
                            if self.role == RaftRole::Follower {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Network receive error during election");
                        }
                    }
                }

                _ = tokio::time::sleep(remaining) => {
                    // Election timeout - start new election
                    tracing::debug!(node_id = self.id, "Election timeout, restarting");
                    self.become_candidate().await;
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Leader loop: send heartbeats, replicate logs
    async fn run_leader(&mut self) -> Result<(), ConsensusError> {
        // Send heartbeats/AppendEntries to all peers
        self.send_append_entries_to_all().await?;

        // Wait for heartbeat interval or incoming message
        tokio::select! {
            result = self.network.receive() => {
                match result {
                    Ok(msg) => self.handle_message(msg).await?,
                    Err(e) => {
                        tracing::warn!(error = %e, "Network receive error");
                    }
                }
            }

            _ = tokio::time::sleep(self.config.heartbeat_interval) => {
                // Time for next heartbeat
            }
        }

        // Advance commit index if possible
        self.advance_commit_index().await?;

        Ok(())
    }

    // ========================================================================
    // MESSAGE HANDLING
    // ========================================================================

    async fn handle_message(&mut self, msg: RaftMessage<T>) -> Result<(), ConsensusError> {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(term, candidate_id, last_log_index, last_log_term)
                    .await?;
            }

            RaftMessage::RequestVoteResponse {
                term,
                vote_granted,
                from_id,
            } => {
                self.handle_request_vote_response(term, vote_granted, from_id)
                    .await?;
            }

            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
                .await?;
            }

            RaftMessage::AppendEntriesResponse {
                term,
                success,
                match_index,
                from_id,
            } => {
                self.handle_append_entries_response(term, success, match_index, from_id)
                    .await?;
            }

            RaftMessage::InstallSnapshot {
                term,
                leader_id,
                snapshot,
            } => {
                self.handle_install_snapshot(term, leader_id, snapshot)
                    .await?;
            }

            RaftMessage::InstallSnapshotResponse {
                term,
                success: _,
                from_id: _,
            } => {
                // Handle snapshot response
                if term > self.storage.get_term().await.unwrap_or(0) {
                    self.become_follower(term).await;
                }
            }
        }

        Ok(())
    }

    // ========================================================================
    // REQUEST VOTE
    // ========================================================================

    async fn handle_request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().await.unwrap_or(0);

        // If term > currentTerm, become follower
        if term > current_term {
            self.become_follower(term).await;
        }

        let current_term = self.storage.get_term().await.unwrap_or(0);
        let voted_for = self.storage.get_vote().await.unwrap_or(None);
        let our_log_info = self.storage.get_last_log_info().await.unwrap_or_default();

        // Grant vote if:
        // 1. term >= currentTerm
        // 2. votedFor is null or candidateId
        // 3. candidate's log is at least as up-to-date as ours
        let vote_granted = term >= current_term
            && (voted_for.is_none() || voted_for == Some(candidate_id))
            && self.is_log_up_to_date(last_log_index, last_log_term, &our_log_info);

        if vote_granted {
            let _ = self.storage.set_vote(Some(candidate_id)).await;
            tracing::debug!(
                node_id = self.id,
                candidate = candidate_id,
                term = term,
                "Granted vote"
            );
        }

        // Send response
        let response = RaftMessage::RequestVoteResponse {
            term: current_term,
            vote_granted,
            from_id: self.id,
        };

        self.network.respond(candidate_id, response).await?;

        Ok(())
    }

    async fn handle_request_vote_response(
        &mut self,
        term: Term,
        vote_granted: bool,
        from_id: NodeId,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().await.unwrap_or(0);

        // If response term > our term, become follower
        if term > current_term {
            self.become_follower(term).await;
            return Ok(());
        }

        // Ignore stale responses
        if term < current_term || self.role != RaftRole::Candidate {
            return Ok(());
        }

        // Record vote
        self.votes_received.insert(from_id, vote_granted);

        if vote_granted {
            let votes = self.votes_received.values().filter(|&&v| v).count();

            tracing::debug!(
                node_id = self.id,
                from = from_id,
                votes = votes,
                "Received vote"
            );

            // Check for majority (Joint Consensus aware)
            if self.has_majority(&self.votes_received) {
                self.become_leader().await;
            }
        }

        Ok(())
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

    // ========================================================================
    // APPEND ENTRIES
    // ========================================================================

    async fn send_append_entries_to_all(&mut self) -> Result<(), ConsensusError> {
        let term = self.storage.get_term().await.unwrap_or(0);
        let peers = self.peer_ids();

        for peer_id in peers {
            if let Err(e) = self.send_append_entries_to_peer(peer_id, term).await {
                tracing::warn!(peer = peer_id, error = %e, "Failed to send AppendEntries");
            }
        }

        Ok(())
    }

    async fn send_append_entries_to_peer(
        &mut self,
        peer_id: NodeId,
        term: Term,
    ) -> Result<(), ConsensusError> {
        let leader_state = self
            .leader_state
            .as_ref()
            .ok_or(ConsensusError::NotLeader)?;

        let next_idx = *leader_state.next_index.get(&peer_id).unwrap_or(&1);
        let prev_log_index = next_idx.saturating_sub(1);

        // Get prev_log_term
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.storage
                .get_log_entry(prev_log_index)
                .await?
                .map(|e| e.term)
                .unwrap_or(0)
        };

        // Get entries to send
        let last_log_info = self.storage.get_last_log_info().await?;
        let end_idx =
            (next_idx + self.config.max_entries_per_rpc as u64).min(last_log_info.last_index + 1);
        let entries_iter = self.storage.get_log_range(next_idx, end_idx).await?;
        let entries: Vec<LogEntry<T>> = entries_iter.collect::<Result<_, _>>()?;

        self.network
            .send_append_entries(
                peer_id,
                term,
                self.id,
                prev_log_index,
                prev_log_term,
                entries,
                self.commit_index,
            )
            .await?;

        Ok(())
    }

    async fn handle_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry<T>>,
        leader_commit: LogIndex,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().await.unwrap_or(0);

        // Reply false if term < currentTerm (§5.1)
        if term < current_term {
            let response = RaftMessage::AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: 0,
                from_id: self.id,
            };
            self.network.respond(leader_id, response).await?;
            return Ok(());
        }

        // If term > currentTerm, become follower
        if term > current_term {
            self.become_follower(term).await;
        }

        // Reset election timer (we received valid AppendEntries from leader)
        self.current_leader = Some(leader_id);

        // If candidate, step down
        if self.role == RaftRole::Candidate {
            self.become_follower(term).await;
        }

        // Check log consistency
        let our_log_info = self.storage.get_last_log_info().await?;

        let success = if prev_log_index == 0 {
            true
        } else if prev_log_index > our_log_info.last_index {
            false
        } else {
            // Check if we have the entry at prev_log_index with matching term
            self.storage
                .get_log_entry(prev_log_index)
                .await?
                .map(|e| e.term == prev_log_term)
                .unwrap_or(false)
        };

        let match_index = if success {
            // Append new entries
            if !entries.is_empty() {
                // Delete conflicting entries and append new ones
                // Delete conflicting entries and append new ones

                // Check for conflicts
                // Check for conflicts
                for entry in &entries {
                    let conflict = match self.storage.get_log_entry(entry.index).await? {
                        Some(existing) => existing.term != entry.term,
                        None => false,
                    };

                    if conflict {
                        // Conflict - delete this and all following
                        self.storage.truncate_log(entry.index).await?;
                        break;
                    }
                }

                // Append entries not already in log
                let log_info = self.storage.get_last_log_info().await?;
                let new_entries: Vec<_> = entries
                    .into_iter()
                    .filter(|e| e.index > log_info.last_index)
                    .collect();

                if !new_entries.is_empty() {
                    self.storage.append_entries(&new_entries).await?;

                    // EXPERT: As soon as a node appends a config entry, it starts using it.
                    for entry in &new_entries {
                        if let LogCommand::Config(config) = &entry.command {
                            tracing::info!(
                                node_id = self.id,
                                index = entry.index,
                                "Node transitioning to new configuration from log"
                            );
                            self.cluster_config = config.clone();

                            // Update network if needed
                            let _ = self
                                .network
                                .update_peers(
                                    config
                                        .all_nodes()
                                        .into_iter()
                                        .map(|id| crate::network::PeerInfo {
                                            id,
                                            address: "".to_string(), // In a real system, we'd have address mapping
                                        })
                                        .collect(),
                                )
                                .await;
                        }
                    }
                }
            }

            // Update commit index
            let our_log_info = self.storage.get_last_log_info().await?;
            if leader_commit > self.commit_index {
                self.commit_index = leader_commit.min(our_log_info.last_index);
                self.storage.set_commit_index(self.commit_index).await?;
            }

            self.storage.get_last_log_info().await?.last_index
        } else {
            0
        };

        // Send response
        let response = RaftMessage::AppendEntriesResponse {
            term: self.storage.get_term().await.unwrap_or(0),
            success,
            match_index,
            from_id: self.id,
        };

        self.network.respond(leader_id, response).await?;

        Ok(())
    }

    async fn handle_append_entries_response(
        &mut self,
        term: Term,
        success: bool,
        match_index: LogIndex,
        from_id: NodeId,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().await.unwrap_or(0);

        // If term > currentTerm, become follower
        if term > current_term {
            self.become_follower(term).await;
            return Ok(());
        }

        // Ignore if not leader or stale term
        if self.role != RaftRole::Leader || term != current_term {
            return Ok(());
        }

        let leader_state = self
            .leader_state
            .as_mut()
            .ok_or(ConsensusError::NotLeader)?;

        if success {
            // Update next_index and match_index for the peer
            if match_index > *leader_state.match_index.get(&from_id).unwrap_or(&0) {
                leader_state.match_index.insert(from_id, match_index);
                leader_state.next_index.insert(from_id, match_index + 1);
            }
        } else {
            // Decrement next_index and retry
            let next_idx = leader_state.next_index.get(&from_id).copied().unwrap_or(1);
            if next_idx > 1 {
                leader_state.next_index.insert(from_id, next_idx - 1);
            }
        }

        Ok(())
    }

    // ========================================================================
    // INSTALL SNAPSHOT
    // ========================================================================

    async fn handle_install_snapshot(
        &mut self,
        term: Term,
        leader_id: NodeId,
        snapshot: crate::raft::Snapshot<T>,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().await.unwrap_or(0);

        if term < current_term {
            let response = RaftMessage::InstallSnapshotResponse {
                term: current_term,
                success: false,
                from_id: self.id,
            };
            self.network.respond(leader_id, response).await?;
            return Ok(());
        }

        if term > current_term {
            self.become_follower(term).await;
        }

        self.current_leader = Some(leader_id);

        // Install the snapshot
        self.storage.install_snapshot(snapshot.clone()).await?;

        // Update commit index
        if snapshot.last_included_index > self.commit_index {
            self.commit_index = snapshot.last_included_index;
            self.storage.set_commit_index(self.commit_index).await?;
        }

        let response = RaftMessage::InstallSnapshotResponse {
            term: self.storage.get_term().await.unwrap_or(0),
            success: true,
            from_id: self.id,
        };

        self.network.respond(leader_id, response).await?;

        Ok(())
    }

    // ========================================================================
    // COMMIT INDEX ADVANCEMENT
    // ========================================================================

    #[tracing::instrument(skip(self), level = "debug")]
    async fn advance_commit_index(&mut self) -> Result<(), ConsensusError> {
        if self.role != RaftRole::Leader {
            return Ok(());
        }

        let leader_state = self
            .leader_state
            .as_ref()
            .ok_or(ConsensusError::NotLeader)?;

        let current_term = self.storage.get_term().await.unwrap_or(0);
        let log_info = self.storage.get_last_log_info().await?;

        // Find the highest N such that:
        // 1. N > commitIndex
        // 2. A majority of matchIndex[i] >= N
        // 3. log[N].term == currentTerm

        let mut joint_entry_to_finalize = None;

        for (i, n) in ((self.commit_index + 1)..=log_info.last_index).enumerate() {
            // Anti-blocking yield
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
            // Check if entry at N has current term
            let entry = self.storage.get_log_entry(n).await?;
            if entry.as_ref().map(|e| e.term) != Some(current_term) {
                continue;
            }

            // Check if majority of current config has replicated this index
            let mut replicated = vec![self.id];
            for (&node_id, &match_idx) in &leader_state.match_index {
                if match_idx >= n {
                    replicated.push(node_id);
                }
            }

            if self.cluster_config.has_majority(&replicated) {
                self.commit_index = n;
                self.storage.set_commit_index(n).await?;

                tracing::debug!(
                    node_id = self.id,
                    commit_index = n,
                    is_joint = self.cluster_config.is_joint(),
                    "Advanced commit index"
                );

                // If we just committed a configuration change, we might need to transition
                if let Some(LogEntry {
                    command: LogCommand::Config(config @ ClusterConfig::Joint { .. }),
                    ..
                }) = entry
                {
                    // Committing EnterJoint -> We should soon transition to LeaveJoint
                    joint_entry_to_finalize = Some(config.clone());
                }
            }
        }

        if let Some(config) = joint_entry_to_finalize {
            self.finalize_conf_change(config).await?;
        }

        Ok(())
    }

    /// Internal method to finalize a Joint Consensus configuration.
    ///
    /// Transitions from `Joint { old, new }` to `Single(new)`.
    async fn finalize_conf_change(
        &mut self,
        joint_config: ClusterConfig,
    ) -> Result<(), ConsensusError> {
        let new_nodes = match joint_config {
            ClusterConfig::Joint { old: _, new } => new,
            _ => {
                return Err(ConsensusError::ConfigChangeError(
                    "Not in Joint state".into(),
                ));
            }
        };

        let final_config = ClusterConfig::Single(new_nodes);
        let term = self.storage.get_term().await.unwrap_or(0);
        let log_info = self.storage.get_last_log_info().await?;
        let next_idx = log_info.last_index + 1;

        let entry = LogEntry::config(next_idx, term, final_config.clone());
        self.storage.append_entries(&[entry]).await?;

        // Update active config immediately
        self.cluster_config = final_config.clone();

        tracing::info!(
            node_id = self.id,
            index = next_idx,
            "Leader finalized configuration to Single state"
        );

        Ok(())
    }
}

#[async_trait]
impl<T, N, S> ConsensusEngine<T> for RaftEngine<T, N, S>
where
    T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    N: RaftNetwork<T> + Send + Sync,
    S: RaftStorage<T> + Send,
{
    async fn run(&mut self) -> Result<(), ConsensusError> {
        self.run_loop().await
    }

    async fn propose(&mut self, value: T) -> Result<LogIndex, ConsensusError> {
        if self.role != RaftRole::Leader {
            return Err(ConsensusError::NotLeader);
        }

        let term = self.storage.get_term().await.unwrap_or(0);
        let log_info = self.storage.get_last_log_info().await?;
        let new_index = log_info.last_index + 1;

        let entry = LogEntry::new(new_index, term, value);

        self.storage.append_entries(&[entry]).await?;

        tracing::debug!(
            node_id = self.id,
            index = new_index,
            term = term,
            "Appended new entry"
        );

        Ok(new_index)
    }

    async fn propose_conf_change(
        &mut self,
        change: crate::raft::ConfChange,
    ) -> Result<LogIndex, ConsensusError> {
        if self.role != RaftRole::Leader {
            return Err(ConsensusError::NotLeader);
        }

        if self.cluster_config.is_joint() {
            return Err(ConsensusError::ConfigChangeInProgress);
        }

        let old_nodes = self.cluster_config.all_nodes();
        let mut new_nodes = old_nodes.clone();
        match change {
            crate::raft::ConfChange::AddNode(id) => {
                if !new_nodes.contains(&id) {
                    new_nodes.push(id);
                }
            }
            crate::raft::ConfChange::RemoveNode(id) => {
                new_nodes.retain(|&x| x != id);
            }
        }

        let joint_config = ClusterConfig::Joint {
            old: old_nodes,
            new: new_nodes,
        };

        let term = self.storage.get_term().await.unwrap_or(0);
        let log_info = self.storage.get_last_log_info().await?;
        let next_idx = log_info.last_index + 1;

        let entry = LogEntry::config(next_idx, term, joint_config.clone());
        self.storage.append_entries(&[entry]).await?;

        // Update active config immediately
        self.cluster_config = joint_config;

        tracing::info!(
            node_id = self.id,
            index = next_idx,
            "Leader proposed Joint Consensus configuration change"
        );

        Ok(next_idx)
    }

    fn leader_id(&self) -> Option<NodeId> {
        self.current_leader
    }

    fn is_leader(&self) -> bool {
        self.role == RaftRole::Leader
    }

    async fn current_term(&self) -> Term {
        self.storage.get_term().await.unwrap_or(0)
    }

    fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
}

// ============================================================================
// LEGACY FACTORY (for backward compatibility)
// ============================================================================

/// Factory for creating consensus engines.
pub struct ConsensusFactory;

impl ConsensusFactory {
    /// Creates a new consensus engine with the specified strategy.
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
                Ok(Box::new(LegacyRaftEngineAdapter::new(id, network, storage)))
            }
            ConsensusStrategy::Paxos => {
                Err(ConsensusError::NotImplemented(ConsensusStrategy::Paxos))
            }
        }
    }
}

/// Legacy adapter for backward compatibility with ConsensusNetwork trait
struct LegacyRaftEngineAdapter<T: Send + Sync> {
    node: crate::raft::RaftNode<T>,
}

impl<T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static> LegacyRaftEngineAdapter<T> {
    fn new(
        id: NodeId,
        network: Box<dyn ConsensusNetwork>,
        storage: Box<dyn RaftStorage<T>>,
    ) -> Self {
        Self {
            node: crate::raft::RaftNode::new(id, network, storage, RaftConfig::default()),
        }
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static> ConsensusEngine<T>
    for LegacyRaftEngineAdapter<T>
{
    async fn run(&mut self) -> Result<(), ConsensusError> {
        tracing::info!(
            node_id = self.node.id,
            "Starting legacy Raft consensus loop"
        );

        loop {
            tokio::task::yield_now().await;
        }
    }

    async fn propose(&mut self, _value: T) -> Result<LogIndex, ConsensusError> {
        // Legacy adapter doesn't actually run, just a placeholder
        Err(ConsensusError::NotLeader)
    }

    async fn propose_conf_change(
        &mut self,
        _change: crate::raft::ConfChange,
    ) -> Result<LogIndex, ConsensusError> {
        Err(ConsensusError::NotLeader)
    }

    fn leader_id(&self) -> Option<NodeId> {
        if self.node.role == RaftRole::Leader {
            Some(self.node.id)
        } else {
            None
        }
    }

    fn is_leader(&self) -> bool {
        self.node.role == RaftRole::Leader
    }

    async fn current_term(&self) -> Term {
        self.node.storage.get_term().await.unwrap_or(0)
    }

    fn commit_index(&self) -> LogIndex {
        self.node.commit_index
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::InMemoryStorage;

    struct MockNetwork;

    #[async_trait]
    impl ConsensusNetwork for MockNetwork {
        async fn broadcast_vote_request(
            &self,
            _term: Term,
            _candidate_id: NodeId,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn send_heartbeat(&self, _leader_id: NodeId, _term: Term) -> Result<(), String> {
            Ok(())
        }

        async fn receive(&self) -> Result<crate::network::Packet, String> {
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

        let result = ConsensusFactory::create_engine(ConsensusStrategy::Raft, 1, network, storage);

        assert!(result.is_ok());
    }

    #[test]
    fn test_paxos_not_implemented() {
        let storage: Box<dyn RaftStorage<String>> = Box::new(InMemoryStorage::new());
        let network: Box<dyn ConsensusNetwork> = Box::new(MockNetwork);

        let result = ConsensusFactory::create_engine(ConsensusStrategy::Paxos, 1, network, storage);

        assert!(matches!(result, Err(ConsensusError::NotImplemented(_))));
    }

    #[test]
    fn test_raft_config_default() {
        let config = RaftConfig::default();
        assert!(config.heartbeat_interval < config.election_timeout_min);
    }

    #[test]
    fn test_raft_config_random_timeout() {
        let config = RaftConfig::default();
        let t1 = config.random_election_timeout();
        let t2 = config.random_election_timeout();

        // Each timeout should be within range
        assert!(t1 >= config.election_timeout_min);
        assert!(t1 <= config.election_timeout_max);
        assert!(t2 >= config.election_timeout_min);
        assert!(t2 <= config.election_timeout_max);
    }
}
