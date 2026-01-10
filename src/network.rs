//! Network transport for Raft consensus.
//!
//! Provides abstract network interface and implementations for consensus messaging.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::raft::{Term, NodeId, LogIndex, LogEntry, Snapshot};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

/// Maximum UDP packet size (theoretical max is 65507, but we use a safer limit)
pub const MAX_PACKET_SIZE: usize = 4096;

/// Default read timeout for UDP sockets
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum backoff duration for supervisor restart
pub const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Initial backoff duration for supervisor restart
pub const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Configuration for network transport
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Size of the receive buffer in bytes
    pub buffer_size: usize,
    /// Read timeout for the socket
    pub read_timeout: Duration,
    /// Initial backoff for supervisor restart
    pub initial_backoff: Duration,
    /// Maximum backoff for supervisor restart
    pub max_backoff: Duration,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            buffer_size: MAX_PACKET_SIZE,
            read_timeout: DEFAULT_READ_TIMEOUT,
            initial_backoff: INITIAL_BACKOFF,
            max_backoff: MAX_BACKOFF,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl NetworkConfig {
    /// Creates a new network configuration with custom values
    pub fn new(buffer_size: usize, read_timeout: Duration) -> Self {
        Self {
            buffer_size: buffer_size.min(MAX_PACKET_SIZE),
            read_timeout,
            ..Default::default()
        }
    }
}

// ============================================================================
// RAFT RPC MESSAGES
// ============================================================================

/// Full Raft RPC message types (per Raft paper §5-7)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage<T> {
    // ===== RequestVote RPC (§5.2) =====
    
    /// Invoked by candidates to gather votes
    RequestVote {
        /// Candidate's term
        term: Term,
        /// Candidate requesting vote
        candidate_id: NodeId,
        /// Index of candidate's last log entry
        last_log_index: LogIndex,
        /// Term of candidate's last log entry
        last_log_term: Term,
    },
    
    /// Response to RequestVote RPC
    RequestVoteResponse {
        /// Current term, for candidate to update itself
        term: Term,
        /// True means candidate received vote
        vote_granted: bool,
        /// Responder's node ID
        from_id: NodeId,
    },
    
    // ===== AppendEntries RPC (§5.3) =====
    
    /// Invoked by leader to replicate log entries; also used as heartbeat
    AppendEntries {
        /// Leader's term
        term: Term,
        /// So follower can redirect clients
        leader_id: NodeId,
        /// Index of log entry immediately preceding new ones
        prev_log_index: LogIndex,
        /// Term of prev_log_index entry
        prev_log_term: Term,
        /// Log entries to store (empty for heartbeat)
        entries: Vec<LogEntry<T>>,
        /// Leader's commit index
        leader_commit: LogIndex,
    },
    
    /// Response to AppendEntries RPC
    AppendEntriesResponse {
        /// Current term, for leader to update itself
        term: Term,
        /// True if follower contained entry matching prev_log_index and prev_log_term
        success: bool,
        /// The index of the last entry replicated (for updating match_index)
        match_index: LogIndex,
        /// Responder's node ID
        from_id: NodeId,
    },
    
    // ===== InstallSnapshot RPC (§7) =====
    
    /// Invoked by leader to send chunks of a snapshot to a follower
    InstallSnapshot {
        /// Leader's term
        term: Term,
        /// So follower can redirect clients
        leader_id: NodeId,
        /// Snapshot data
        snapshot: Snapshot<T>,
    },
    
    /// Response to InstallSnapshot RPC
    InstallSnapshotResponse {
        /// Current term, for leader to update itself
        term: Term,
        /// True if snapshot was accepted
        success: bool,
        /// Responder's node ID
        from_id: NodeId,
    },
}

/// Legacy packet type for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Packet {
    VoteRequest {
        term: Term,
        candidate_id: NodeId,
    },
    VoteResponse {
        term: Term,
        vote_granted: bool,
    },
    Heartbeat {
        leader_id: NodeId,
        term: Term,
    },
    /// Configuration change (membership change)
    ConfigChange {
        /// Type of change: "add" or "remove"
        change_type: String,
        /// Address of the peer
        peer_address: String,
        /// Node ID
        node_id: NodeId,
    },
}

// ============================================================================
// NETWORK TRAIT
// ============================================================================

/// Peer information for network transport
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: NodeId,
    pub address: String,
}

/// Abstract network interface for Raft consensus.
///
/// Handles sending/receiving Raft RPC messages to/from peers.
#[async_trait]
pub trait RaftNetwork<T: Send + Sync + Clone>: Send + Sync {
    /// Sends a RequestVote RPC to a specific peer.
    async fn send_request_vote(
        &self,
        peer_id: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Result<Option<RaftMessage<T>>, NetworkError>;
    
    /// Sends an AppendEntries RPC to a specific peer.
    async fn send_append_entries(
        &self,
        peer_id: NodeId,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry<T>>,
        leader_commit: LogIndex,
    ) -> Result<Option<RaftMessage<T>>, NetworkError>;
    
    /// Sends an InstallSnapshot RPC to a specific peer.
    async fn send_install_snapshot(
        &self,
        peer_id: NodeId,
        term: Term,
        leader_id: NodeId,
        snapshot: Snapshot<T>,
    ) -> Result<Option<RaftMessage<T>>, NetworkError>;
    
    /// Receives the next incoming RPC message.
    async fn receive(&self) -> Result<RaftMessage<T>, NetworkError>;
    
    /// Responds to an incoming RPC.
    async fn respond(&self, to: NodeId, message: RaftMessage<T>) -> Result<(), NetworkError>;
    
    /// Gets the list of peer IDs.
    fn peer_ids(&self) -> Vec<NodeId>;
    
    /// Updates the peer list.
    async fn update_peers(&self, peers: Vec<PeerInfo>) -> Result<(), NetworkError>;
}

/// Network errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum NetworkError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Timeout")]
    Timeout,
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Peer not found: {0}")]
    PeerNotFound(NodeId),
    #[error("Transport error: {0}")]
    TransportError(String),
}

/// Legacy ConsensusNetwork trait for backward compatibility
#[async_trait]
pub trait ConsensusNetwork: Send + Sync {
    /// Broadcast a RequestVote RPC to all peers.
    async fn broadcast_vote_request(&self, term: Term, candidate_id: NodeId) -> Result<(), String>;
    
    /// Send a Heartbeat (empty AppendEntries) to all peers.
    async fn send_heartbeat(&self, leader_id: NodeId, term: Term) -> Result<(), String>;
    
    /// Receive the next packet from the network.
    async fn receive(&self) -> Result<Packet, String>;

    /// Update the list of peers (for dynamic membership).
    async fn update_peers(&self, peers: Vec<String>) -> Result<(), String>;
}

// ============================================================================
// IN-MEMORY NETWORK (for testing)
// ============================================================================

/// In-memory network transport for testing.
pub struct InMemoryNetwork<T> {
    node_id: NodeId,
    peers: Arc<tokio::sync::RwLock<HashMap<NodeId, PeerInfo>>>,
    inbox: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<RaftMessage<T>>>>,
    outboxes: Arc<tokio::sync::RwLock<HashMap<NodeId, tokio::sync::mpsc::Sender<RaftMessage<T>>>>>,
}

impl<T: Send + Sync + Clone + 'static> InMemoryNetwork<T> {
    /// Creates a new in-memory network node.
    pub fn new(
        node_id: NodeId,
        inbox_rx: tokio::sync::mpsc::Receiver<RaftMessage<T>>,
    ) -> Self {
        Self {
            node_id,
            peers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            inbox: Arc::new(tokio::sync::Mutex::new(inbox_rx)),
            outboxes: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }
    
    /// Registers a peer's outbox for sending messages.
    pub async fn register_peer(&self, peer_id: NodeId, address: String, sender: tokio::sync::mpsc::Sender<RaftMessage<T>>) {
        self.peers.write().await.insert(peer_id, PeerInfo { id: peer_id, address });
        self.outboxes.write().await.insert(peer_id, sender);
    }
}

#[async_trait]
impl<T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned + 'static> RaftNetwork<T> for InMemoryNetwork<T> {
    async fn send_request_vote(
        &self,
        peer_id: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Result<Option<RaftMessage<T>>, NetworkError> {
        let outboxes = self.outboxes.read().await;
        let sender = outboxes.get(&peer_id)
            .ok_or_else(|| NetworkError::PeerNotFound(peer_id))?;
        
        sender.send(RaftMessage::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }).await.map_err(|e| NetworkError::TransportError(e.to_string()))?;
        
        Ok(None) // Response comes via inbox
    }
    
    async fn send_append_entries(
        &self,
        peer_id: NodeId,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry<T>>,
        leader_commit: LogIndex,
    ) -> Result<Option<RaftMessage<T>>, NetworkError> {
        let outboxes = self.outboxes.read().await;
        let sender = outboxes.get(&peer_id)
            .ok_or_else(|| NetworkError::PeerNotFound(peer_id))?;
        
        sender.send(RaftMessage::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }).await.map_err(|e| NetworkError::TransportError(e.to_string()))?;
        
        Ok(None)
    }
    
    async fn send_install_snapshot(
        &self,
        peer_id: NodeId,
        term: Term,
        leader_id: NodeId,
        snapshot: Snapshot<T>,
    ) -> Result<Option<RaftMessage<T>>, NetworkError> {
        let outboxes = self.outboxes.read().await;
        let sender = outboxes.get(&peer_id)
            .ok_or_else(|| NetworkError::PeerNotFound(peer_id))?;
        
        sender.send(RaftMessage::InstallSnapshot {
            term,
            leader_id,
            snapshot,
        }).await.map_err(|e| NetworkError::TransportError(e.to_string()))?;
        
        Ok(None)
    }
    
    async fn receive(&self) -> Result<RaftMessage<T>, NetworkError> {
        let mut inbox = self.inbox.lock().await;
        inbox.recv().await.ok_or(NetworkError::TransportError("Channel closed".into()))
    }
    
    async fn respond(&self, to: NodeId, message: RaftMessage<T>) -> Result<(), NetworkError> {
        let outboxes = self.outboxes.read().await;
        let sender = outboxes.get(&to)
            .ok_or_else(|| NetworkError::PeerNotFound(to))?;
        
        sender.send(message).await.map_err(|e| NetworkError::TransportError(e.to_string()))
    }
    
    fn peer_ids(&self) -> Vec<NodeId> {
        // Blocking read - only for simple cases
        Vec::new()
    }
    
    async fn update_peers(&self, peers: Vec<PeerInfo>) -> Result<(), NetworkError> {
        let mut peer_map = self.peers.write().await;
        peer_map.clear();
        for peer in peers {
            peer_map.insert(peer.id, peer);
        }
        Ok(())
    }
}

// ============================================================================
// UDP TRANSPORT (legacy)
// ============================================================================

#[cfg(feature = "net")]
pub mod udp {
    use super::*;
    use tokio::net::UdpSocket;
    use tokio::sync::RwLock;

    /// UDP-based network transport for consensus algorithms.
    pub struct UdpTransport {
        socket: Arc<UdpSocket>,
        peers: Arc<RwLock<Vec<String>>>,
        config: NetworkConfig,
    }

    impl std::fmt::Debug for UdpTransport {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("UdpTransport")
                .field("peers", &self.peers)
                .field("config", &self.config)
                .finish_non_exhaustive()
        }
    }

    impl UdpTransport {
        /// Creates a new UDP transport with default configuration.
        pub async fn new(bind_addr: &str, peers: Vec<String>) -> Result<Self, String> {
            Self::with_config(bind_addr, peers, NetworkConfig::default()).await
        }

        /// Creates a new UDP transport with custom configuration.
        pub async fn with_config(
            bind_addr: &str,
            peers: Vec<String>,
            config: NetworkConfig,
        ) -> Result<Self, String> {
            let socket = UdpSocket::bind(bind_addr).await.map_err(|e| {
                format!("Failed to bind UDP socket to '{}': {}", bind_addr, e)
            })?;

            tracing::info!(
                bind_addr = bind_addr,
                peer_count = peers.len(),
                buffer_size = config.buffer_size,
                "Async UDP transport initialized"
            );

            Ok(Self {
                socket: Arc::new(socket),
                peers: Arc::new(RwLock::new(peers)),
                config,
            })
        }
    }

    #[async_trait]
    impl ConsensusNetwork for UdpTransport {
        async fn broadcast_vote_request(&self, term: Term, candidate_id: NodeId) -> Result<(), String> {
            let packet = Packet::VoteRequest { term, candidate_id };
            let serialized = serde_json::to_vec(&packet).map_err(|e| e.to_string())?;

            let peers = self.peers.read().await;
            for peer in peers.iter() {
                let _ = self.socket.send_to(&serialized, peer).await.map_err(|e| {
                    tracing::warn!("Failed to send to {}: {}", peer, e);
                    e
                });
            }
            Ok(())
        }

        async fn send_heartbeat(&self, leader_id: NodeId, term: Term) -> Result<(), String> {
            let packet = Packet::Heartbeat { leader_id, term };
            let serialized = serde_json::to_vec(&packet).map_err(|e| e.to_string())?;

            let peers = self.peers.read().await;
            for peer in peers.iter() {
                let _ = self.socket.send_to(&serialized, peer).await;
            }
            Ok(())
        }

        async fn receive(&self) -> Result<Packet, String> {
            let mut buf = vec![0u8; self.config.buffer_size];
            
            loop {
                match tokio::time::timeout(self.config.read_timeout, self.socket.recv_from(&mut buf)).await {
                    Ok(io_result) => {
                        match io_result {
                            Ok((amt, _src)) => {
                                let packet: Packet = serde_json::from_slice(&buf[..amt])
                                    .map_err(|e| e.to_string())?;
                                return Ok(packet);
                            }
                            Err(e) => {
                                tracing::error!("UDP receive IO error: {}", e);
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                    Err(_) => {
                        // Timeout - continue loop
                    }
                }
            }
        }

        async fn update_peers(&self, new_peers: Vec<String>) -> Result<(), String> {
            let mut peers = self.peers.write().await;
            *peers = new_peers;
            tracing::info!(peer_count = peers.len(), "Updated ConsensusNetwork peers");
            Ok(())
        }
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_config_default() {
        let config = NetworkConfig::default();
        assert_eq!(config.buffer_size, MAX_PACKET_SIZE);
        assert_eq!(config.read_timeout, DEFAULT_READ_TIMEOUT);
    }

    #[test]
    fn test_network_config_clamps_buffer_size() {
        let config = NetworkConfig::new(100_000, Duration::from_millis(50));
        assert_eq!(config.buffer_size, MAX_PACKET_SIZE);
    }

    #[test]
    fn test_packet_serialization() {
        let packet = Packet::VoteRequest {
            term: 1,
            candidate_id: 42,
        };
        let serialized = serde_json::to_vec(&packet).unwrap();
        let deserialized: Packet = serde_json::from_slice(&serialized).unwrap();
        
        match deserialized {
            Packet::VoteRequest { term, candidate_id } => {
                assert_eq!(term, 1);
                assert_eq!(candidate_id, 42);
            }
            _ => panic!("Wrong packet type"),
        }
    }

    #[cfg(feature = "net")]
    mod udp_tests {
        use super::super::*;
        use super::super::udp::UdpTransport;
        
        #[tokio::test]
        async fn test_invalid_bind_address() {
            let result = UdpTransport::new("not-an-address", vec![]).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_valid_bind_address() {
            let result = UdpTransport::new("127.0.0.1:0", vec![]).await;
            assert!(result.is_ok());
        }
    }
}
