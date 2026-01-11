//! State Machine Abstraction
//!
//! Provides the interface for user-defined state machines that can be replicated via Raft.

use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

/// Abstract state machine that can be replicated via Raft.
///
/// The state machine receives commands from the Raft log and applies them
/// to produce outputs. It also supports snapshotting for log compaction.
///
/// # Type Parameters
///
/// - `Command`: The command type that modifies state
/// - `Output`: The result of applying a command
/// - `SnapshotData`: The serialized snapshot format
///
/// # Example
///
/// ```ignore
/// use praborrow_lease::state_machine::StateMachine;
///
/// struct KeyValueStore {
///     data: HashMap<String, String>,
/// }
///
/// impl StateMachine for KeyValueStore {
///     type Command = KvCommand;
///     type Output = Option<String>;
///     type SnapshotData = HashMap<String, String>;
///
///     fn apply(&mut self, command: Self::Command) -> Self::Output {
///         match command {
///             KvCommand::Set { key, value } => {
///                 self.data.insert(key, value)
///             }
///             KvCommand::Get { key } => {
///                 self.data.get(&key).cloned()
///             }
///             KvCommand::Delete { key } => {
///                 self.data.remove(&key)
///             }
///         }
///     }
///
///     fn snapshot(&self) -> Self::SnapshotData {
///         self.data.clone()
///     }
///
///     fn restore(&mut self, snapshot: Self::SnapshotData) {
///         self.data = snapshot;
///     }
/// }
/// ```
pub trait StateMachine: Send + Sync {
    /// The command type that modifies the state machine.
    type Command: Clone + Send + Sync + Serialize + DeserializeOwned + Debug;

    /// The output produced when applying a command.
    type Output: Clone + Send + Sync + Serialize + DeserializeOwned + Debug;

    /// The snapshot data format.
    type SnapshotData: Clone + Send + Sync + Serialize + DeserializeOwned;

    /// Applies a command to the state machine, returning the result.
    ///
    /// This method must be deterministic - the same command applied to the
    /// same state must always produce the same result.
    fn apply(&mut self, command: Self::Command) -> Self::Output;

    /// Creates a snapshot of the current state.
    ///
    /// This is used for log compaction. The snapshot must capture
    /// the complete state as of the last applied command.
    fn snapshot(&self) -> Self::SnapshotData;

    /// Restores the state machine from a snapshot.
    ///
    /// Called when installing a snapshot from the leader.
    fn restore(&mut self, snapshot: Self::SnapshotData);

    /// Returns the name of this state machine (for logging/metrics).
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// A simple no-op state machine for testing.
#[derive(Debug, Default, Clone)]
pub struct NoOpStateMachine;

impl StateMachine for NoOpStateMachine {
    type Command = Vec<u8>;
    type Output = ();
    type SnapshotData = ();

    fn apply(&mut self, _command: Self::Command) -> Self::Output {}

    fn snapshot(&self) -> Self::SnapshotData {}

    fn restore(&mut self, _snapshot: Self::SnapshotData) {
        // No state to restore
    }

    fn name(&self) -> &str {
        "NoOpStateMachine"
    }
}

/// A key-value store state machine for testing and examples.
#[derive(Debug, Clone)]
pub struct KeyValueStateMachine {
    data: std::collections::HashMap<String, Vec<u8>>,
}

impl Default for KeyValueStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStateMachine {
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Commands for the key-value state machine.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub enum KvCommand {
    /// Set a key to a value
    Set { key: String, value: Vec<u8> },
    /// Delete a key
    Delete { key: String },
}

/// Output for key-value commands.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub enum KvOutput {
    /// Previous value (if any) for Set/Delete
    Value(Option<Vec<u8>>),
}

impl StateMachine for KeyValueStateMachine {
    type Command = KvCommand;
    type Output = KvOutput;
    type SnapshotData = std::collections::HashMap<String, Vec<u8>>;

    fn apply(&mut self, command: Self::Command) -> Self::Output {
        match command {
            KvCommand::Set { key, value } => {
                let old = self.data.insert(key, value);
                KvOutput::Value(old)
            }
            KvCommand::Delete { key } => {
                let old = self.data.remove(&key);
                KvOutput::Value(old)
            }
        }
    }

    fn snapshot(&self) -> Self::SnapshotData {
        self.data.clone()
    }

    fn restore(&mut self, snapshot: Self::SnapshotData) {
        self.data = snapshot;
    }

    fn name(&self) -> &str {
        "KeyValueStateMachine"
    }
}

// ============================================================================
// REPLICATED STATE MACHINE
// ============================================================================

use crate::engine::ConsensusError;
use crate::raft::{LogIndex, RaftStorage};

/// A replicated state machine that applies committed log entries.
pub struct ReplicatedStateMachine<SM: StateMachine, S: RaftStorage<SM::Command>>
where
    SM::Command: Send + Sync,
{
    /// The underlying state machine
    state_machine: SM,
    /// Storage for accessing committed entries
    storage: S,
    /// Last applied log index
    last_applied: LogIndex,
}

impl<SM, S> ReplicatedStateMachine<SM, S>
where
    SM: StateMachine,
    SM::Command: Send + Sync,
    S: RaftStorage<SM::Command>,
{
    /// Creates a new replicated state machine.
    pub fn new(state_machine: SM, storage: S) -> Self {
        Self {
            state_machine,
            storage,
            last_applied: 0,
        }
    }

    /// Returns a reference to the underlying state machine.
    pub fn state_machine(&self) -> &SM {
        &self.state_machine
    }

    /// Returns the last applied index.
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Applies all committed entries up to commit_index.
    ///
    /// Returns the outputs for each applied command.
    pub async fn apply_committed(
        &mut self,
        commit_index: LogIndex,
    ) -> Result<Vec<(LogIndex, SM::Output)>, ConsensusError> {
        let mut outputs = Vec::new();

        while self.last_applied < commit_index {
            let next_index = self.last_applied + 1;

            if let Some(entry) = self.storage.get_log_entry(next_index).await? {
                let output = self.state_machine.apply(entry.command);
                outputs.push((next_index, output));
                self.last_applied = next_index;

                tracing::debug!(
                    index = next_index,
                    sm = self.state_machine.name(),
                    "Applied log entry"
                );
            } else {
                // Entry not found - might be compacted
                break;
            }
        }

        Ok(outputs)
    }

    /// Creates a snapshot of the current state.
    pub fn create_snapshot(&self) -> SM::SnapshotData {
        self.state_machine.snapshot()
    }

    /// Restores from a snapshot.
    pub fn restore_snapshot(&mut self, snapshot: SM::SnapshotData, last_included_index: LogIndex) {
        self.state_machine.restore(snapshot);
        self.last_applied = last_included_index;

        tracing::info!(
            index = last_included_index,
            sm = self.state_machine.name(),
            "Restored from snapshot"
        );
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_state_machine() {
        let mut sm = NoOpStateMachine;
        let output = sm.apply(vec![1, 2, 3]);
        assert_eq!(output, ());

        let snapshot = sm.snapshot();
        sm.restore(snapshot);
    }

    #[test]
    fn test_kv_state_machine_set() {
        let mut sm = KeyValueStateMachine::new();

        let output = sm.apply(KvCommand::Set {
            key: "foo".to_string(),
            value: b"bar".to_vec(),
        });

        assert!(matches!(output, KvOutput::Value(None)));
        assert_eq!(sm.get("foo"), Some(&b"bar".to_vec()));
    }

    #[test]
    fn test_kv_state_machine_overwrite() {
        let mut sm = KeyValueStateMachine::new();

        sm.apply(KvCommand::Set {
            key: "foo".to_string(),
            value: b"bar".to_vec(),
        });

        let output = sm.apply(KvCommand::Set {
            key: "foo".to_string(),
            value: b"baz".to_vec(),
        });

        assert!(matches!(output, KvOutput::Value(Some(_))));
        assert_eq!(sm.get("foo"), Some(&b"baz".to_vec()));
    }

    #[test]
    fn test_kv_state_machine_delete() {
        let mut sm = KeyValueStateMachine::new();

        sm.apply(KvCommand::Set {
            key: "foo".to_string(),
            value: b"bar".to_vec(),
        });

        let output = sm.apply(KvCommand::Delete {
            key: "foo".to_string(),
        });

        assert!(matches!(output, KvOutput::Value(Some(_))));
        assert!(sm.get("foo").is_none());
    }

    #[test]
    fn test_kv_state_machine_snapshot() {
        let mut sm = KeyValueStateMachine::new();

        sm.apply(KvCommand::Set {
            key: "a".to_string(),
            value: b"1".to_vec(),
        });
        sm.apply(KvCommand::Set {
            key: "b".to_string(),
            value: b"2".to_vec(),
        });

        let snapshot = sm.snapshot();
        assert_eq!(snapshot.len(), 2);

        // Modify state
        sm.apply(KvCommand::Delete {
            key: "a".to_string(),
        });
        assert_eq!(sm.len(), 1);

        // Restore from snapshot
        sm.restore(snapshot);
        assert_eq!(sm.len(), 2);
        assert_eq!(sm.get("a"), Some(&b"1".to_vec()));
    }
}
