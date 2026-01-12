use crate::engine::{ConsensusError, RaftConfig};
use crate::network::ConsensusNetwork;
use crate::raft::{FileStorage, InMemoryStorage, NodeId, RaftNode, RaftStorage};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;

/// Builder for `RaftNode`.
///
/// Provides a fluent API for configuring and creating a `RaftNode`.
pub struct RaftNodeBuilder<T> {
    id: Option<NodeId>,
    storage: Option<Box<dyn RaftStorage<T>>>,
    network: Option<Box<dyn ConsensusNetwork>>,
    config: RaftConfig,
    _phantom: PhantomData<T>,
}

impl<T> RaftNodeBuilder<T>
where
    T: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Creates a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            id: None,
            storage: None,
            network: None,
            config: RaftConfig::default(),
            _phantom: PhantomData,
        }
    }

    /// Sets the node ID (Required).
    pub fn id(mut self, id: u128) -> Self {
        self.id = Some(id);
        self
    }

    /// Configures the node to use in-memory storage (Default if not specified, but explicit is better).
    pub fn with_memory_storage(mut self) -> Self {
        self.storage = Some(Box::new(InMemoryStorage::new()));
        self
    }

    /// Configures the node to use persistent file storage (Sled).
    ///
    /// # feature="std" required
    #[cfg(feature = "std")]
    pub fn with_sled_storage(mut self, path: PathBuf) -> Result<Self, ConsensusError> {
        // metrics are optional, passing None for now as per requirement simplicity
        let storage = FileStorage::open(path, None)?;
        self.storage = Some(Box::new(storage));
        Ok(self)
    }

    /// Sets the network transport (Required).
    pub fn with_network(mut self, network: impl ConsensusNetwork + 'static) -> Self {
        self.network = Some(Box::new(network));
        self
    }

    /// Modifies the configuration via a closure.
    pub fn configure<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut RaftConfig),
    {
        f(&mut self.config);
        self
    }

    /// Builds the `RaftNode`.
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError::StorageError` (wrapper usually) if required fields are missing.
    /// Specifically using `ConsensusError::NetworkError` or similar for missing fields as Generic error.
    pub fn build(self) -> Result<RaftNode<T>, ConsensusError> {
        let id = self.id.ok_or_else(|| {
            ConsensusError::NetworkError("Node ID is required for RaftNode".to_string())
        })?;

        let network = self.network.ok_or_else(|| {
            ConsensusError::NetworkError("Network transport is required for RaftNode".to_string())
        })?;

        let storage = self
            .storage
            .unwrap_or_else(|| Box::new(InMemoryStorage::new()));

        Ok(RaftNode::new(id, network, storage, self.config))
    }
}

impl<T> Default for RaftNodeBuilder<T>
where
    T: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
