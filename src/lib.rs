//! Lease Consensus Logic
//!
//! This crate implements the consensus algorithms required to agree on Lease Validity.

mod manager;
pub mod raft;
pub mod network;
pub mod engine;

pub use manager::LeaseManager;
