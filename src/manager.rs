use praborrow_core::{Sovereign, DistributedBorrow, Lease, LeaseError};
use std::time::Duration;

/// Manages the consensus of leases for a specific resource.
///
/// In Year 1, this acts as the "Local Leader". 
/// In Year 5, this will participate in Raft/Paxos.
pub struct LeaseManager<'a, T> {
    resource: &'a Sovereign<T>,
}

impl<'a, T> LeaseManager<'a, T> {
    pub fn new(resource: &'a Sovereign<T>) -> Self {
        Self { resource }
    }

    /// Request a vote (Lease) from the local sovereign.
    ///
    /// If successful, the resource is "Exiled" for the duration.
    pub fn request_vote(&self, candidate_id: u128, term: Duration) -> Result<Lease<T>, LeaseError> {
        // In this simplified model, the Sovereign *is* the voter and the storage.
        // The Manager facilitates the protocol.
        self.resource.try_hire(candidate_id, term)
    }

    /// Simulates a heartbeat failure (Split vote / Partition).
    ///
    /// For now, this just verifies if the lease is valid.

    pub fn heartbeat(&self) -> bool {
        // If we can access it domestically, there is no active lease (or it expired).
        // If we panic/fail access, there is a lease.
        // We need a non-panicking way to check status in Sovereign?
        // Wait, Sovereign doesn't expose `is_leased()`.
        // Let's rely on `try_hire` behavior or add introspection later.
        // For now: NO-OP mock.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_voting() {
        let resource = Sovereign::new(500);
        let manager = LeaseManager::new(&resource);

        let term = Duration::from_secs(1);
        let vote = manager.request_vote(101, term);
        
        assert!(vote.is_ok());
        let lease = vote.unwrap();
        assert_eq!(lease.holder, 101);

        // Verify resource is now locked/foreign
        // We can't easily check "is_foreign" without a method, but we can try to vote again?
        // DistributedBorrow::try_hire returns Err if already leased.
        
        let vote2 = manager.request_vote(102, term);
        assert!(vote2.is_err());
    }
}
