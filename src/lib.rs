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
//! - `observability` - Enable Prometheus metrics and OpenTelemetry tracing
//! - `full` - Enable all features

mod manager;
pub mod raft;
pub mod network;
pub mod engine;
pub mod metrics;

pub use manager::LeaseManager;
pub use engine::{ConsensusEngine, ConsensusError, ConsensusFactory, ConsensusStrategy, RaftConfig, RaftEngine};
pub use network::{RaftNetwork, RaftMessage, NetworkError, NetworkConfig, PeerInfo, InMemoryNetwork};
pub use network::{ConsensusNetwork, Packet};
pub use metrics::{RaftMetrics, RaftRoleMetric};
pub use raft::{
    RaftNode, RaftRole, RaftStorage, InMemoryStorage, FileStorage,
    Term, NodeId, LogIndex, LogEntry, LogInfo, Snapshot, StorageStats,
};
