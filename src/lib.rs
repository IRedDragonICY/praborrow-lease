//! Lease Consensus Logic
//!
//! This crate implements the consensus algorithms required to agree on Lease Validity.

mod manager;
pub mod raft;

pub use manager::LeaseManager;
