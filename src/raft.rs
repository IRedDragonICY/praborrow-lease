use serde::{Serialize, Deserialize};
use crate::network::ConsensusNetwork;
use std::boxed::Box;

pub type Term = u64;
pub type NodeId = u128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub term: Term,
    pub command: T,
}

/// Abstract storage interface for Raft state persistence.
pub trait RaftStorage<T> {
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), String>;
    fn get_term(&self) -> Result<Term, String>;
    fn set_term(&mut self, term: Term) -> Result<(), String>;
    fn get_vote(&self) -> Result<Option<NodeId>, String>;
    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), String>;
    fn get_log(&self) -> Result<&[LogEntry<T>], String>;
}

/// Default in-memory storage implementation.
pub struct InMemoryStorage<T> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<T>>,
}

impl<T> InMemoryStorage<T> {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl<T: Clone> RaftStorage<T> for InMemoryStorage<T> {
    fn append_entries(&mut self, entries: &[LogEntry<T>]) -> Result<(), String> {
        self.log.extend_from_slice(entries);
        Ok(())
    }

    fn get_term(&self) -> Result<Term, String> {
        Ok(self.current_term)
    }

    fn set_term(&mut self, term: Term) -> Result<(), String> {
        self.current_term = term;
        Ok(())
    }

    fn get_vote(&self) -> Result<Option<NodeId>, String> {
        Ok(self.voted_for)
    }

    fn set_vote(&mut self, vote: Option<NodeId>) -> Result<(), String> {
        self.voted_for = vote;
        Ok(())
    }

    fn get_log(&self) -> Result<&[LogEntry<T>], String> {
        Ok(&self.log)
    }
}

/// The Raft State Machine.
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

impl<T: Clone + 'static> RaftNode<T> {
    pub fn new(id: NodeId, network: Box<dyn ConsensusNetwork>) -> Self {
        Self {
            storage: Box::new(InMemoryStorage::new()),
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            id,
            network,
        }
    }

    /// Transition to Candidate and start election.
    pub async fn start_election(&mut self) {
        let current_term = self.storage.get_term().unwrap_or(0);
        let new_term = current_term + 1;
        
        let _ = self.storage.set_term(new_term);
        let _ = self.storage.set_vote(Some(self.id));
        
        self.role = RaftRole::Candidate;
        
        // Broadcast RequestVote
        if let Err(e) = self.network.broadcast_vote_request(new_term, self.id).await {
            // In a real system, we'd log this error
            let _ = e; 
        }
    }

    /// Handle RequestVote RPC.
    pub fn handle_request_vote(&mut self, term: Term, candidate_id: NodeId) -> bool {
        let current_term = self.storage.get_term().unwrap_or(0);
        let voted_for = self.storage.get_vote().unwrap_or(None);

        if term > current_term {
            let _ = self.storage.set_term(term);
            self.role = RaftRole::Follower;
            let _ = self.storage.set_vote(None);
        }

        if term < current_term {
            return false;
        }

        if voted_for.is_none() || voted_for == Some(candidate_id)
           // && Candidate's log is at least as up-to-date as receiver's log (simplified)
        {
            let _ = self.storage.set_vote(Some(candidate_id));
            return true;
        }

        false
    }
}
