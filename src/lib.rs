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

pub mod engine;
mod manager;
pub mod metrics;
pub mod network;
pub mod deadlock;
pub mod raft;
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
    ConsensusEngine, ConsensusError, ConsensusFactory, ConsensusStrategy, RaftConfig, RaftEngine,
};

// Network exports
pub use network::{
    ConsensusNetwork, InMemoryNetwork, NetworkConfig, NetworkError, Packet, PeerInfo, RaftMessage,
    RaftNetwork,
};

// Metrics exports
pub use metrics::{RaftMetrics, RaftRoleMetric};

// Raft core exports
pub use raft::{
    FileStorage, InMemoryStorage, LogEntry, LogIndex, LogInfo, NodeId, RaftNode, RaftRole,
    RaftStorage, Snapshot, StorageStats, Term,
};

// State machine exports
pub use state_machine::{
    KeyValueStateMachine, KvCommand, KvOutput, NoOpStateMachine, ReplicatedStateMachine,
    StateMachine,
};

// gRPC exports (when feature enabled)
#[cfg(feature = "grpc")]
pub use grpc::{
    CircuitBreaker, CircuitBreakerConfig, CircuitState, ConnectionPool, GrpcConfig, GrpcTransport,
    TlsConfig, start_grpc_server,
};

// ============================================================================
// PRELUDE
// ============================================================================

/// Convenient imports for common use cases.
pub mod prelude {
    pub use crate::{
        ConsensusEngine, ConsensusError, FileStorage, InMemoryNetwork, InMemoryStorage,
        KeyValueStateMachine, LogEntry, LogIndex, NodeId, RaftConfig, RaftEngine, RaftMessage,
        RaftNetwork, RaftStorage, ReplicatedStateMachine, StateMachine, Term,
    };
}
