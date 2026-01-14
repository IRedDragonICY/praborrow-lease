use dashmap::DashMap;
use praborrow_core::{DistributedBorrow, Lease, LeaseError, Sovereign};
use std::time::{Duration, Instant};

/// Manages the consensus of leases for a specific resource.
///
/// In Year 1, this acts as the "Local Leader".
/// In Year 5, this will participate in Raft/Paxos.
pub struct LeaseManager<'a, T> {
    resource: &'a Sovereign<T>,
    active_leases: DashMap<u128, Instant>,
}

impl<'a, T> LeaseManager<'a, T> {
    pub fn new(resource: &'a Sovereign<T>) -> Self {
        Self {
            resource,
            active_leases: DashMap::new(),
        }
    }

    /// Request a vote (Lease) from the local sovereign.
    ///
    /// If successful, the resource is "Exiled" for the duration.
    pub fn request_vote(&self, candidate_id: u128, term: Duration) -> Result<Lease<T>, LeaseError> {
        // In this simplified model, the Sovereign *is* the voter and the storage.
        // The Manager facilitates the protocol.
        let lease = self.resource.try_hire(candidate_id, term)?;

        // Track the lease
        self.active_leases
            .insert(candidate_id, Instant::now() + term);

        Ok(lease)
    }

    /// Simulates a heartbeat failure (Split vote / Partition).
    ///
    /// Checks for expired leases.
    /// Returns `false` if any lease has expired (indicating a potential violation/zombie lease).
    pub fn heartbeat(&self) -> bool {
        let now = Instant::now();
        let mut all_healthy = true;

        // Remove expired leases
        self.active_leases.retain(|id, expiry| {
            if *expiry < now {
                tracing::warn!(candidate_id = id, "Lease expired detected by heartbeat");
                all_healthy = false;
                false // remove from map
            } else {
                true
            }
        });

        // Note: Ideally we should inform Sovereign to repatriate, but Sovereign logic
        // handles its own timing or requires explicit return.
        // For now, we report health status.

        all_healthy
    }

    /// Processing a returned lease and issuing a repatriation token.
    ///
    /// This consumes the lease (conceptually) and returns the token required
    /// to restore the resource to domestic jurisdiction.
    ///
    /// # Safety
    ///
    /// The manager assumes that if the lease is being returned, the remote
    /// holder has truly relinquished it.
    pub fn return_lease(&self, lease: Lease<T>) -> praborrow_core::RepatriationToken {
        // Remove from tracking
        self.active_leases.remove(&lease.holder);

        // In a real system, we'd verify signatures/messages here.
        // For now, we trust the Lease object proves it was held.
        unsafe { praborrow_core::RepatriationToken::new(lease.holder) }
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

    #[test]
    fn test_heartbeat_expiration() {
        let resource = Sovereign::new(100);
        let manager = LeaseManager::new(&resource);

        // Lease with very short duration
        let _ = manager.request_vote(202, Duration::from_millis(1));

        std::thread::sleep(Duration::from_millis(10));

        // Heartbeat should return false (expired)
        assert!(!manager.heartbeat());

        // Map should be empty
        assert!(manager.active_leases.is_empty());
    }
}
