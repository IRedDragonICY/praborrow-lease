//! Consensus Engine Implementation
//!
//! Provides the full Raft consensus loop with leader election, log replication,
//! and commit index advancement.

use crate::network::{RaftNetwork, RaftMessage, NetworkError, ConsensusNetwork};
use crate::raft::{NodeId, Term, LogIndex, RaftStorage, RaftRole, LogEntry, LogInfo};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use rand::Rng;

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Raft timing configuration
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Minimum election timeout (randomized between min and max)
    pub election_timeout_min: Duration,
    /// Maximum election timeout
    pub election_timeout_max: Duration,
    /// Heartbeat interval (must be << election timeout)
    pub heartbeat_interval: Duration,
    /// RPC timeout
    pub rpc_timeout: Duration,
    /// Max entries per AppendEntries RPC
    pub max_entries_per_rpc: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(100),
            max_entries_per_rpc: 100,
        }
    }
}

impl RaftConfig {
    /// Returns a randomized election timeout
    pub fn random_election_timeout(&self) -> Duration {
        let min = self.election_timeout_min.as_millis() as u64;
        let max = self.election_timeout_max.as_millis() as u64;
        let timeout_ms = rand::rng().random_range(min..=max);
        Duration::from_millis(timeout_ms)
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
    #[error("Shutdown requested")]
    Shutdown,
}

impl From<NetworkError> for ConsensusError {
    fn from(e: NetworkError) -> Self {
        ConsensusError::NetworkError(e.to_string())
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
    
    /// Returns the current leader ID, if known.
    fn leader_id(&self) -> Option<NodeId>;
    
    /// Returns true if this node is the leader.
    fn is_leader(&self) -> bool;
    
    /// Returns the current term.
    fn current_term(&self) -> Term;
    
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
        
        Self { next_index, match_index }
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
    last_applied: LogIndex,
    
    // Leader state (None if not leader)
    leader_state: Option<LeaderState>,
    
    // Known leader (for redirecting clients)
    current_leader: Option<NodeId>,
    
    // Network transport
    network: N,
    
    // Configuration
    config: RaftConfig,
    
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
    pub fn new(id: NodeId, network: N, storage: S, config: RaftConfig) -> Self {
        let commit_index = storage.get_commit_index().unwrap_or(0);
        
        tracing::info!(
            node_id = id,
            commit_index = commit_index,
            "Creating Raft engine"
        );
        
        Self {
            id,
            storage,
            role: RaftRole::Follower,
            commit_index,
            last_applied: 0,
            leader_state: None,
            current_leader: None,
            network,
            config,
            votes_received: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Returns peer IDs from network
    fn peer_ids(&self) -> Vec<NodeId> {
        self.network.peer_ids()
    }
    
    /// Returns the majority quorum size
    fn quorum_size(&self) -> usize {
        let total = self.peer_ids().len() + 1; // +1 for self
        (total / 2) + 1
    }
    
    // ========================================================================
    // ROLE TRANSITIONS
    // ========================================================================
    
    fn become_follower(&mut self, term: Term) {
        let old_role = self.role.clone();
        self.role = RaftRole::Follower;
        self.leader_state = None;
        self.votes_received.clear();
        
        let _ = self.storage.set_term(term);
        let _ = self.storage.set_vote(None);
        
        if old_role != RaftRole::Follower {
            tracing::info!(
                node_id = self.id,
                from_role = %old_role,
                new_term = term,
                "Became follower"
            );
        }
    }
    
    fn become_candidate(&mut self) {
        let current_term = self.storage.get_term().unwrap_or(0);
        let new_term = current_term + 1;
        
        self.role = RaftRole::Candidate;
        self.leader_state = None;
        self.current_leader = None;
        self.votes_received.clear();
        
        // Vote for self
        let _ = self.storage.set_term_and_vote(new_term, Some(self.id));
        self.votes_received.insert(self.id, true);
        
        tracing::info!(
            node_id = self.id,
            term = new_term,
            "Became candidate, starting election"
        );
    }
    
    fn become_leader(&mut self) {
        let term = self.storage.get_term().unwrap_or(0);
        let log_info = self.storage.get_last_log_info().unwrap_or_default();
        
        self.role = RaftRole::Leader;
        self.current_leader = Some(self.id);
        self.leader_state = Some(LeaderState::new(&self.peer_ids(), log_info.last_index));
        self.votes_received.clear();
        
        tracing::info!(
            node_id = self.id,
            term = term,
            "Became leader"
        );
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
                self.become_candidate();
            }
        }
        
        Ok(())
    }
    
    /// Candidate loop: request votes, collect responses
    async fn run_candidate(&mut self) -> Result<(), ConsensusError> {
        let term = self.storage.get_term().unwrap_or(0);
        let log_info = self.storage.get_last_log_info().unwrap_or_default();
        
        // Send RequestVote to all peers
        for peer_id in self.peer_ids() {
            let _ = self.network.send_request_vote(
                peer_id,
                term,
                self.id,
                log_info.last_index,
                log_info.last_term,
            ).await;
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
                    self.become_candidate();
                    return Ok(());
                }
            }
        }
        
        Ok(())
    }
    
    /// Leader loop: send heartbeats, replicate logs
    async fn run_leader(&mut self) -> Result<(), ConsensusError> {
        let term = self.storage.get_term().unwrap_or(0);
        
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
        self.advance_commit_index()?;
        
        Ok(())
    }
    
    // ========================================================================
    // MESSAGE HANDLING
    // ========================================================================
    
    async fn handle_message(&mut self, msg: RaftMessage<T>) -> Result<(), ConsensusError> {
        match msg {
            RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                self.handle_request_vote(term, candidate_id, last_log_index, last_log_term).await?;
            }
            
            RaftMessage::RequestVoteResponse { term, vote_granted, from_id } => {
                self.handle_request_vote_response(term, vote_granted, from_id)?;
            }
            
            RaftMessage::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                self.handle_append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit).await?;
            }
            
            RaftMessage::AppendEntriesResponse { term, success, match_index, from_id } => {
                self.handle_append_entries_response(term, success, match_index, from_id)?;
            }
            
            RaftMessage::InstallSnapshot { term, leader_id, snapshot } => {
                self.handle_install_snapshot(term, leader_id, snapshot).await?;
            }
            
            RaftMessage::InstallSnapshotResponse { term, success, from_id } => {
                // Handle snapshot response
                if term > self.storage.get_term().unwrap_or(0) {
                    self.become_follower(term);
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
        let current_term = self.storage.get_term().unwrap_or(0);
        
        // If term > currentTerm, become follower
        if term > current_term {
            self.become_follower(term);
        }
        
        let current_term = self.storage.get_term().unwrap_or(0);
        let voted_for = self.storage.get_vote().unwrap_or(None);
        let our_log_info = self.storage.get_last_log_info().unwrap_or_default();
        
        // Grant vote if:
        // 1. term >= currentTerm
        // 2. votedFor is null or candidateId
        // 3. candidate's log is at least as up-to-date as ours
        let vote_granted = term >= current_term
            && (voted_for.is_none() || voted_for == Some(candidate_id))
            && self.is_log_up_to_date(last_log_index, last_log_term, &our_log_info);
        
        if vote_granted {
            let _ = self.storage.set_vote(Some(candidate_id));
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
    
    fn handle_request_vote_response(
        &mut self,
        term: Term,
        vote_granted: bool,
        from_id: NodeId,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().unwrap_or(0);
        
        // If response term > our term, become follower
        if term > current_term {
            self.become_follower(term);
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
                needed = self.quorum_size(),
                "Received vote"
            );
            
            // Check for majority
            if votes >= self.quorum_size() {
                self.become_leader();
            }
        }
        
        Ok(())
    }
    
    /// Checks if candidate's log is at least as up-to-date as ours (§5.4.1)
    fn is_log_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term, our_log: &LogInfo) -> bool {
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
        let term = self.storage.get_term().unwrap_or(0);
        let peers = self.peer_ids();
        
        for peer_id in peers {
            if let Err(e) = self.send_append_entries_to_peer(peer_id, term).await {
                tracing::warn!(peer = peer_id, error = %e, "Failed to send AppendEntries");
            }
        }
        
        Ok(())
    }
    
    async fn send_append_entries_to_peer(&mut self, peer_id: NodeId, term: Term) -> Result<(), ConsensusError> {
        let leader_state = self.leader_state.as_ref()
            .ok_or(ConsensusError::NotLeader)?;
        
        let next_idx = *leader_state.next_index.get(&peer_id).unwrap_or(&1);
        let prev_log_index = next_idx.saturating_sub(1);
        
        // Get prev_log_term
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.storage.get_log_entry(prev_log_index)?
                .map(|e| e.term)
                .unwrap_or(0)
        };
        
        // Get entries to send
        let last_log_info = self.storage.get_last_log_info()?;
        let end_idx = (next_idx + self.config.max_entries_per_rpc as u64).min(last_log_info.last_index + 1);
        let entries = self.storage.get_log_range(next_idx, end_idx)?;
        
        self.network.send_append_entries(
            peer_id,
            term,
            self.id,
            prev_log_index,
            prev_log_term,
            entries,
            self.commit_index,
        ).await?;
        
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
        let current_term = self.storage.get_term().unwrap_or(0);
        
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
            self.become_follower(term);
        }
        
        // Reset election timer (we received valid AppendEntries from leader)
        self.current_leader = Some(leader_id);
        
        // If candidate, step down
        if self.role == RaftRole::Candidate {
            self.become_follower(term);
        }
        
        // Check log consistency
        let our_log_info = self.storage.get_last_log_info()?;
        
        let success = if prev_log_index == 0 {
            true
        } else if prev_log_index > our_log_info.last_index {
            false
        } else {
            // Check if we have the entry at prev_log_index with matching term
            self.storage.get_log_entry(prev_log_index)?
                .map(|e| e.term == prev_log_term)
                .unwrap_or(false)
        };
        
        let match_index = if success {
            // Append new entries
            if !entries.is_empty() {
                // Delete conflicting entries and append new ones
                let first_new_index = entries.first().map(|e| e.index).unwrap_or(prev_log_index + 1);
                
                // Check for conflicts
                for entry in &entries {
                    if let Some(existing) = self.storage.get_log_entry(entry.index)? {
                        if existing.term != entry.term {
                            // Conflict - delete this and all following
                            self.storage.truncate_log(entry.index)?;
                            break;
                        }
                    }
                }
                
                // Append entries not already in log
                let log_info = self.storage.get_last_log_info()?;
                let new_entries: Vec<_> = entries.into_iter()
                    .filter(|e| e.index > log_info.last_index)
                    .collect();
                
                if !new_entries.is_empty() {
                    self.storage.append_entries(&new_entries)?;
                }
            }
            
            // Update commit index
            let our_log_info = self.storage.get_last_log_info()?;
            if leader_commit > self.commit_index {
                self.commit_index = leader_commit.min(our_log_info.last_index);
                self.storage.set_commit_index(self.commit_index)?;
            }
            
            self.storage.get_last_log_info()?.last_index
        } else {
            0
        };
        
        // Send response
        let response = RaftMessage::AppendEntriesResponse {
            term: self.storage.get_term().unwrap_or(0),
            success,
            match_index,
            from_id: self.id,
        };
        
        self.network.respond(leader_id, response).await?;
        
        Ok(())
    }
    
    fn handle_append_entries_response(
        &mut self,
        term: Term,
        success: bool,
        match_index: LogIndex,
        from_id: NodeId,
    ) -> Result<(), ConsensusError> {
        let current_term = self.storage.get_term().unwrap_or(0);
        
        // If term > currentTerm, become follower
        if term > current_term {
            self.become_follower(term);
            return Ok(());
        }
        
        // Ignore if not leader or stale term
        if self.role != RaftRole::Leader || term != current_term {
            return Ok(());
        }
        
        let leader_state = self.leader_state.as_mut()
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
        let current_term = self.storage.get_term().unwrap_or(0);
        
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
            self.become_follower(term);
        }
        
        self.current_leader = Some(leader_id);
        
        // Install the snapshot
        self.storage.install_snapshot(snapshot.clone())?;
        
        // Update commit index
        if snapshot.last_included_index > self.commit_index {
            self.commit_index = snapshot.last_included_index;
            self.storage.set_commit_index(self.commit_index)?;
        }
        
        let response = RaftMessage::InstallSnapshotResponse {
            term: self.storage.get_term().unwrap_or(0),
            success: true,
            from_id: self.id,
        };
        
        self.network.respond(leader_id, response).await?;
        
        Ok(())
    }
    
    // ========================================================================
    // COMMIT INDEX ADVANCEMENT
    // ========================================================================
    
    fn advance_commit_index(&mut self) -> Result<(), ConsensusError> {
        if self.role != RaftRole::Leader {
            return Ok(());
        }
        
        let leader_state = self.leader_state.as_ref()
            .ok_or(ConsensusError::NotLeader)?;
        
        let current_term = self.storage.get_term().unwrap_or(0);
        let log_info = self.storage.get_last_log_info()?;
        
        // Find the highest N such that:
        // 1. N > commitIndex
        // 2. A majority of matchIndex[i] >= N
        // 3. log[N].term == currentTerm
        
        for n in (self.commit_index + 1)..=log_info.last_index {
            // Check if entry at N has current term
            let entry = self.storage.get_log_entry(n)?;
            if entry.as_ref().map(|e| e.term) != Some(current_term) {
                continue;
            }
            
            // Count replicas
            let mut count = 1; // Count self
            for (&_peer, &match_idx) in &leader_state.match_index {
                if match_idx >= n {
                    count += 1;
                }
            }
            
            if count >= self.quorum_size() {
                self.commit_index = n;
                self.storage.set_commit_index(n)?;
                
                tracing::debug!(
                    node_id = self.id,
                    commit_index = n,
                    "Advanced commit index"
                );
            }
        }
        
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
        
        let term = self.storage.get_term().unwrap_or(0);
        let log_info = self.storage.get_last_log_info()?;
        let new_index = log_info.last_index + 1;
        
        let entry = LogEntry {
            term,
            index: new_index,
            command: value,
        };
        
        self.storage.append_entries(&[entry])?;
        
        tracing::debug!(
            node_id = self.id,
            index = new_index,
            term = term,
            "Appended new entry"
        );
        
        Ok(new_index)
    }
    
    fn leader_id(&self) -> Option<NodeId> {
        self.current_leader
    }
    
    fn is_leader(&self) -> bool {
        self.role == RaftRole::Leader
    }
    
    fn current_term(&self) -> Term {
        self.storage.get_term().unwrap_or(0)
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
struct LegacyRaftEngineAdapter<T> {
    node: crate::raft::RaftNode<T>,
}

impl<T: Clone + Send + Serialize + DeserializeOwned + 'static> LegacyRaftEngineAdapter<T> {
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
impl<T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static> ConsensusEngine<T> for LegacyRaftEngineAdapter<T> {
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
        if self.node.role != RaftRole::Leader {
            return Err(ConsensusError::NotLeader);
        }
        
        let log_info = self.node.storage.get_last_log_info()?;
        Ok(log_info.last_index + 1)
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
    
    fn current_term(&self) -> Term {
        self.node.storage.get_term().unwrap_or(0)
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
        async fn broadcast_vote_request(&self, _term: Term, _candidate_id: NodeId) -> Result<(), String> {
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
