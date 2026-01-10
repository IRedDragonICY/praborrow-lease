//! Lease Consensus Logic
//!
//! This crate implements the consensus algorithms required to agree on Lease Validity.
//!
//! # Features
//!
//! - `std` - Enable standard library (includes tokio)
//! - `net` - Enable networking (UDP transport)
//! - `grpc` - Enable gRPC transport with tonic
//! - `tls` - Enable TLS/mTLS support
//! - `observability` - Enable Prometheus metrics
//! - `full` - Enable all features
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     ConsensusEngine                         │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
//! │  │  RaftEngine │  │ RaftNetwork │  │ ReplicatedStateMachine│ │
//! │  └─────────────┘  └─────────────┘  └─────────────────────┘ │
//! │         │               │                    │              │
//! │         ▼               ▼                    ▼              │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
//! │  │ RaftStorage │  │GrpcTransport│  │   StateMachine      │ │
//! │  │ (Sled/Mem)  │  │ (or InMem)  │  │   (User-defined)    │ │
//! │  └─────────────┘  └─────────────┘  └─────────────────────┘ │
//! └─────────────────────────────────────────────────────────────┘
//! ```

mod manager;
pub mod raft;
pub mod network;
pub mod engine;
pub mod metrics;
pub mod state_machine;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(test)]
mod cluster_test;

// ============================================================================
// RE-EXPORTS
// ============================================================================

pub use manager::LeaseManager;

// Engine exports
pub use engine::{
    ConsensusEngine, ConsensusError, ConsensusFactory, ConsensusStrategy, 
    RaftConfig, RaftEngine,
};

// Network exports
pub use network::{
    RaftNetwork, RaftMessage, NetworkError, NetworkConfig, PeerInfo, InMemoryNetwork,
    ConsensusNetwork, Packet,
};

// Metrics exports
pub use metrics::{RaftMetrics, RaftRoleMetric};

// Raft core exports
pub use raft::{
    RaftNode, RaftRole, RaftStorage, InMemoryStorage, FileStorage,
    Term, NodeId, LogIndex, LogEntry, LogInfo, Snapshot, StorageStats,
};

// State machine exports
pub use state_machine::{
    StateMachine, NoOpStateMachine, KeyValueStateMachine, KvCommand, KvOutput,
    ReplicatedStateMachine,
};

// gRPC exports (when feature enabled)
#[cfg(feature = "grpc")]
pub use grpc::{
    GrpcTransport, GrpcConfig, TlsConfig, CircuitBreaker, CircuitBreakerConfig,
    CircuitState, ConnectionPool, start_grpc_server,
};

// ============================================================================
// PRELUDE
// ============================================================================

/// Convenient imports for common use cases.
pub mod prelude {
    pub use crate::{
        ConsensusEngine, ConsensusError, RaftConfig, RaftEngine,
        RaftNetwork, RaftMessage, InMemoryNetwork,
        RaftStorage, InMemoryStorage, FileStorage,
        StateMachine, KeyValueStateMachine, ReplicatedStateMachine,
        Term, NodeId, LogIndex, LogEntry,
    };
}
