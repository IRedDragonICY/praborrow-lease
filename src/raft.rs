use serde::{Serialize, Deserialize};

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

/// The Raft State Machine.
pub struct RaftNode<T> {
    // Persistent State
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry<T>>,

    // Volatile State
    pub commit_index: usize,
    pub last_applied: usize,
    
    // Node State
    pub role: RaftRole,
    pub id: NodeId,
}

impl<T> RaftNode<T> {
    pub fn new(id: NodeId) -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            id,
        }
    }

    /// Transition to Candidate and start election.
    pub fn start_election(&mut self) {
        self.current_term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(self.id);
        // In a real impl, we would reset election timer and broadcast RequestVote here.
    }

    /// Handle RequestVote RPC.
    pub fn handle_request_vote(&mut self, term: Term, candidate_id: NodeId) -> bool {
        if term > self.current_term {
            self.current_term = term;
            self.role = RaftRole::Follower;
            self.voted_for = None;
        }

        if term < self.current_term {
            return false;
        }

        if self.voted_for.is_none() || self.voted_for == Some(candidate_id)
           // && Candidate's log is at least as up-to-date as receiver's log (simplified)
        {
            self.voted_for = Some(candidate_id);
            return true;
        }

        false
    }
}
