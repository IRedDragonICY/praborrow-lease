//! Raft Metrics and Observability
//!
//! Provides Prometheus metrics and OpenTelemetry tracing for Raft consensus monitoring.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Raft metrics for Prometheus export.
///
/// These metrics follow the naming conventions from the Prometheus best practices.
#[derive(Debug, Clone)]
pub struct RaftMetrics {
    inner: Arc<RaftMetricsInner>,
}

#[derive(Debug)]
struct RaftMetricsInner {
    /// Node ID label
    node_id: u128,

    // Gauges
    term: AtomicU64,
    role: AtomicU64, // 0=follower, 1=candidate, 2=leader
    commit_index: AtomicU64,
    last_applied: AtomicU64,
    log_entries: AtomicU64,
    peer_count: AtomicU64,

    // Counters
    elections_total: AtomicU64,
    elections_won: AtomicU64,
    elections_lost: AtomicU64,
    proposals_total: AtomicU64,
    proposals_failed: AtomicU64,

    // Leader-specific
    heartbeats_sent: AtomicU64,
    append_entries_sent: AtomicU64,
    append_entries_received: AtomicU64,

    // Timing (stored as nanoseconds)
    last_heartbeat_ns: AtomicI64,
    election_timeout_ns: AtomicU64,

    // Histograms (Observability)
    #[cfg(feature = "observability")]
    disk_write_duration: prometheus::Histogram,
    #[cfg(feature = "observability")]
    rpc_latency: prometheus::HistogramVec,
}

impl RaftMetrics {
    /// Creates a new metrics instance for the given node.
    /// Creates a new metrics instance for the given node.
    pub fn new(node_id: u128) -> Self {
        Self {
            inner: Arc::new(RaftMetricsInner {
                node_id,
                term: AtomicU64::new(0),
                role: AtomicU64::new(0),
                commit_index: AtomicU64::new(0),
                last_applied: AtomicU64::new(0),
                log_entries: AtomicU64::new(0),
                peer_count: AtomicU64::new(0),
                elections_total: AtomicU64::new(0),
                elections_won: AtomicU64::new(0),
                elections_lost: AtomicU64::new(0),
                proposals_total: AtomicU64::new(0),
                proposals_failed: AtomicU64::new(0),
                heartbeats_sent: AtomicU64::new(0),
                append_entries_sent: AtomicU64::new(0),
                append_entries_received: AtomicU64::new(0),
                last_heartbeat_ns: AtomicI64::new(0),
                election_timeout_ns: AtomicU64::new(0),
                #[cfg(feature = "observability")]
                disk_write_duration: prometheus::register_histogram!(
                    "raft_disk_write_duration_seconds",
                    "Disk write duration in seconds",
                    prometheus::DEFAULT_BUCKETS.to_vec()
                )
                .unwrap_or_else(|_| {
                    prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                        "raft_disk_write_duration_seconds",
                        "Disk write duration in seconds",
                    ))
                    .unwrap()
                }),
                #[cfg(feature = "observability")]
                rpc_latency: prometheus::register_histogram_vec!(
                    "raft_rpc_latency_seconds",
                    "RPC latency in seconds",
                    &["method"]
                )
                .unwrap_or_else(|_| {
                    prometheus::HistogramVec::new(
                        prometheus::HistogramOpts::new(
                            "raft_rpc_latency_seconds",
                            "RPC latency in seconds",
                        ),
                        &["method"],
                    )
                    .unwrap()
                }),
            }),
        }
    }

    /// Returns the node ID for this metrics instance.
    pub fn node_id(&self) -> u128 {
        self.inner.node_id
    }

    // ========================================================================
    // GAUGE SETTERS
    // ========================================================================

    /// Sets the current term.
    pub fn set_term(&self, term: u64) {
        self.inner.term.store(term, Ordering::Relaxed);
    }

    /// Sets the current role (0=follower, 1=candidate, 2=leader).
    pub fn set_role(&self, role: RaftRoleMetric) {
        self.inner.role.store(role as u64, Ordering::Relaxed);
    }

    /// Sets the commit index.
    pub fn set_commit_index(&self, index: u64) {
        self.inner.commit_index.store(index, Ordering::Relaxed);
    }

    /// Sets the last applied index.
    pub fn set_last_applied(&self, index: u64) {
        self.inner.last_applied.store(index, Ordering::Relaxed);
    }

    /// Sets the log entries count.
    pub fn set_log_entries(&self, count: u64) {
        self.inner.log_entries.store(count, Ordering::Relaxed);
    }

    /// Sets the peer count.
    pub fn set_peer_count(&self, count: u64) {
        self.inner.peer_count.store(count, Ordering::Relaxed);
    }

    // ========================================================================
    // COUNTER INCREMENTERS
    // ========================================================================

    /// Increments the total elections counter.
    pub fn inc_elections(&self) {
        self.inner.elections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the elections won counter.
    pub fn inc_elections_won(&self) {
        self.inner.elections_won.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the elections lost counter.
    pub fn inc_elections_lost(&self) {
        self.inner.elections_lost.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the proposals counter.
    pub fn inc_proposals(&self) {
        self.inner.proposals_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the failed proposals counter.
    pub fn inc_proposals_failed(&self) {
        self.inner.proposals_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the heartbeats sent counter.
    pub fn inc_heartbeats_sent(&self) {
        self.inner.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the append entries sent counter.
    pub fn inc_append_entries_sent(&self) {
        self.inner
            .append_entries_sent
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the append entries received counter.
    pub fn inc_append_entries_received(&self) {
        self.inner
            .append_entries_received
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // TIMING
    // ========================================================================

    /// Records the last heartbeat timestamp (nanoseconds since epoch).
    pub fn record_heartbeat(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        self.inner.last_heartbeat_ns.store(now, Ordering::Relaxed);
    }

    /// Sets the election timeout (nanoseconds).
    pub fn set_election_timeout(&self, timeout_ns: u64) {
        self.inner
            .election_timeout_ns
            .store(timeout_ns, Ordering::Relaxed);
    }

    // ========================================================================
    // OBSERVABILITY
    // ========================================================================

    /// Observes a disk write duration.
    pub fn observe_disk_write(&self, duration: std::time::Duration) {
        let _ = duration;
        #[cfg(feature = "observability")]
        self.inner
            .disk_write_duration
            .observe(duration.as_secs_f64());
    }

    /// Observes an RPC latency.
    pub fn observe_rpc(&self, method: &str, duration: std::time::Duration) {
        let _ = duration;
        let _ = method;
        #[cfg(feature = "observability")]
        self.inner
            .rpc_latency
            .with_label_values(&[method])
            .observe(duration.as_secs_f64());
    }

    // ========================================================================
    // GETTERS (for export)
    // ========================================================================

    pub fn term(&self) -> u64 {
        self.inner.term.load(Ordering::Relaxed)
    }
    pub fn role(&self) -> u64 {
        self.inner.role.load(Ordering::Relaxed)
    }
    pub fn commit_index(&self) -> u64 {
        self.inner.commit_index.load(Ordering::Relaxed)
    }
    pub fn last_applied(&self) -> u64 {
        self.inner.last_applied.load(Ordering::Relaxed)
    }
    pub fn log_entries(&self) -> u64 {
        self.inner.log_entries.load(Ordering::Relaxed)
    }
    pub fn peer_count(&self) -> u64 {
        self.inner.peer_count.load(Ordering::Relaxed)
    }
    pub fn elections_total(&self) -> u64 {
        self.inner.elections_total.load(Ordering::Relaxed)
    }
    pub fn elections_won(&self) -> u64 {
        self.inner.elections_won.load(Ordering::Relaxed)
    }
    pub fn elections_lost(&self) -> u64 {
        self.inner.elections_lost.load(Ordering::Relaxed)
    }
    pub fn proposals_total(&self) -> u64 {
        self.inner.proposals_total.load(Ordering::Relaxed)
    }
    pub fn proposals_failed(&self) -> u64 {
        self.inner.proposals_failed.load(Ordering::Relaxed)
    }
    pub fn heartbeats_sent(&self) -> u64 {
        self.inner.heartbeats_sent.load(Ordering::Relaxed)
    }
    pub fn append_entries_sent(&self) -> u64 {
        self.inner.append_entries_sent.load(Ordering::Relaxed)
    }
    pub fn append_entries_received(&self) -> u64 {
        self.inner.append_entries_received.load(Ordering::Relaxed)
    }

    /// Returns the replication lag (commit_index - last_applied).
    pub fn replication_lag(&self) -> u64 {
        let commit = self.commit_index();
        let applied = self.last_applied();
        commit.saturating_sub(applied)
    }

    /// Returns time since last heartbeat in milliseconds.
    pub fn time_since_heartbeat_ms(&self) -> i64 {
        let last = self.inner.last_heartbeat_ns.load(Ordering::Relaxed);
        if last == 0 {
            return -1; // No heartbeat received yet
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        (now - last) / 1_000_000
    }

    // ========================================================================
    // PROMETHEUS EXPORT
    // ========================================================================

    /// Exports metrics in Prometheus text format.
    pub fn to_prometheus_text(&self) -> String {
        let node_id = self.node_id();

        let mut output = String::new();

        // Helper macro for metrics
        macro_rules! gauge {
            ($name:expr, $help:expr, $value:expr) => {
                output.push_str(&format!(
                    "# HELP {} {}\n# TYPE {} gauge\n{}{{node_id=\"{}\"}} {}\n",
                    $name, $help, $name, $name, node_id, $value
                ));
            };
        }

        macro_rules! counter {
            ($name:expr, $help:expr, $value:expr) => {
                output.push_str(&format!(
                    "# HELP {} {}\n# TYPE {} counter\n{}{{node_id=\"{}\"}} {}\n",
                    $name, $help, $name, $name, node_id, $value
                ));
            };
        }

        // Gauges
        gauge!("raft_term", "Current Raft term", self.term());
        gauge!(
            "raft_role",
            "Current role (0=follower, 1=candidate, 2=leader)",
            self.role()
        );
        gauge!(
            "raft_commit_index",
            "Current commit index",
            self.commit_index()
        );
        gauge!(
            "raft_last_applied",
            "Last applied log index",
            self.last_applied()
        );
        gauge!(
            "raft_log_entries",
            "Number of log entries",
            self.log_entries()
        );
        gauge!("raft_peer_count", "Number of peers", self.peer_count());
        gauge!(
            "raft_replication_lag",
            "Replication lag (commit - applied)",
            self.replication_lag()
        );

        // Counters
        counter!(
            "raft_elections_total",
            "Total number of elections",
            self.elections_total()
        );
        counter!(
            "raft_elections_won",
            "Number of elections won",
            self.elections_won()
        );
        counter!(
            "raft_elections_lost",
            "Number of elections lost",
            self.elections_lost()
        );
        counter!(
            "raft_proposals_total",
            "Total proposals received",
            self.proposals_total()
        );
        counter!(
            "raft_proposals_failed",
            "Failed proposals",
            self.proposals_failed()
        );
        counter!(
            "raft_heartbeats_sent",
            "Heartbeats sent (leader only)",
            self.heartbeats_sent()
        );
        counter!(
            "raft_append_entries_sent",
            "AppendEntries RPCs sent",
            self.append_entries_sent()
        );
        counter!(
            "raft_append_entries_received",
            "AppendEntries RPCs received",
            self.append_entries_received()
        );

        output
    }
}

/// Role representation for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum RaftRoleMetric {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

impl RaftRoleMetric {
    pub fn from_role(role: u64) -> Self {
        match role {
            0 => RaftRoleMetric::Follower,
            1 => RaftRoleMetric::Candidate,
            2 => RaftRoleMetric::Leader,
            _ => RaftRoleMetric::Follower,
        }
    }
}

// ============================================================================
// TRACING HELPERS
// ============================================================================

/// Creates a new span for Raft RPC operations.
#[macro_export]
macro_rules! raft_span {
    ($name:expr, $($field:tt)*) => {
        tracing::info_span!($name, $($field)*)
    };
}

/// Records an election event.
pub fn trace_election_started(node_id: u128, term: u64) {
    tracing::info!(
        node_id = node_id,
        term = term,
        event = "election_started",
        "Starting election"
    );
}

/// Records an election win.
pub fn trace_election_won(node_id: u128, term: u64, votes: usize) {
    tracing::info!(
        node_id = node_id,
        term = term,
        votes = votes,
        event = "election_won",
        "Won election"
    );
}

/// Records becoming leader.
pub fn trace_became_leader(node_id: u128, term: u64) {
    tracing::info!(
        node_id = node_id,
        term = term,
        event = "became_leader",
        "Became leader"
    );
}

/// Records receiving a heartbeat.
pub fn trace_heartbeat_received(node_id: u128, leader_id: u128, term: u64) {
    tracing::debug!(
        node_id = node_id,
        leader_id = leader_id,
        term = term,
        event = "heartbeat_received",
        "Received heartbeat"
    );
}

/// Records commit index advancement.
pub fn trace_commit_advanced(node_id: u128, old_index: u64, new_index: u64) {
    tracing::info!(
        node_id = node_id,
        old_index = old_index,
        new_index = new_index,
        event = "commit_advanced",
        "Advanced commit index"
    );
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = RaftMetrics::new(1);
        assert_eq!(metrics.node_id(), 1);
        assert_eq!(metrics.term(), 0);
        assert_eq!(metrics.role(), 0);
    }

    #[test]
    fn test_metrics_setters() {
        let metrics = RaftMetrics::new(1);

        metrics.set_term(5);
        assert_eq!(metrics.term(), 5);

        metrics.set_role(RaftRoleMetric::Leader);
        assert_eq!(metrics.role(), 2);

        metrics.set_commit_index(100);
        assert_eq!(metrics.commit_index(), 100);
    }

    #[test]
    fn test_metrics_counters() {
        let metrics = RaftMetrics::new(1);

        metrics.inc_elections();
        metrics.inc_elections();
        assert_eq!(metrics.elections_total(), 2);

        metrics.inc_elections_won();
        assert_eq!(metrics.elections_won(), 1);
    }

    #[test]
    fn test_replication_lag() {
        let metrics = RaftMetrics::new(1);

        metrics.set_commit_index(100);
        metrics.set_last_applied(90);

        assert_eq!(metrics.replication_lag(), 10);
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = RaftMetrics::new(42);
        metrics.set_term(5);
        metrics.set_role(RaftRoleMetric::Leader);
        metrics.inc_elections();

        let output = metrics.to_prometheus_text();

        assert!(output.contains("raft_term{node_id=\"42\"} 5"));
        assert!(output.contains("raft_role{node_id=\"42\"} 2"));
        assert!(output.contains("raft_elections_total{node_id=\"42\"} 1"));
    }

    #[test]
    fn test_role_metric_conversion() {
        assert_eq!(RaftRoleMetric::from_role(0), RaftRoleMetric::Follower);
        assert_eq!(RaftRoleMetric::from_role(1), RaftRoleMetric::Candidate);
        assert_eq!(RaftRoleMetric::from_role(2), RaftRoleMetric::Leader);
    }
}
