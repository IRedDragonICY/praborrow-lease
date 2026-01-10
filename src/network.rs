use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::raft::{Term, NodeId};
use std::time::Duration;

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
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            buffer_size: MAX_PACKET_SIZE,
            read_timeout: DEFAULT_READ_TIMEOUT,
            initial_backoff: INITIAL_BACKOFF,
            max_backoff: MAX_BACKOFF,
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

/// A network packet for consensus algorithms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Packet {
    VoteRequest {
        term: Term,
        candidate_id: NodeId,
        // In real Raft, we also need last_log_index, last_log_term
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
        /// Address of the peer (or NodeId if we map it)
        peer_address: String,
        /// Node ID (optional if we are just adding by address, but mostly we need both or generate ID)
        /// For simplicity, we'll assume address is unique and we'll hash it or provided ID.
        node_id: NodeId,
    },
}

/// Abstract network interface for Consensus/Raft.
///
/// Handles sending messages to peers and receiving incoming packets.
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

#[cfg(feature = "net")]
pub mod udp {
    use super::*;
    use tokio::net::UdpSocket;
    use std::sync::Arc;
    use std::io::ErrorKind;

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
                // simple send_to, ignoring errors for individual peers for now to match partial semantics,
                // but ideally we should track them.
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
                // Use tokio::select! or timeout based on config
                // Since UdpSocket receive is cancellable, we can just await it.
                // However, we want to respect the read_timeout from config if possible, 
                // but typically in async we rely on external timeouts. 
                // For now, simple await.
                
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
                                // Continue loop on error? Or retun?
                                // Usually transient errors should be ignored?
                                // For IO error, maybe brief backoff
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                    Err(_) => {
                        // Timeout
                        // Just continue loop
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
            // Error message depends on OS/implementation, but typically contains "invalid" or similar
            // Let's just check it is error
        }

        #[tokio::test]
        async fn test_valid_bind_address() {
            // Use port 0 to let OS assign an available port
            let result = UdpTransport::new("127.0.0.1:0", vec![]).await;
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_udp_transport_loopback() {
            // Setup Node 1
            let node1 = UdpTransport::new("127.0.0.1:0", vec![]).await.expect("Failed to create node1");
            let addr1 = node1.socket.local_addr().unwrap();

            // Setup Node 2
            // Node 2 knows about Node 1
            let node2 = UdpTransport::new("127.0.0.1:0", vec![addr1.to_string()]).await.expect("Failed to create node2");
            let addr2 = node2.socket.local_addr().unwrap();

            // Node 1 needs to know about Node 2 to reply/broadcast? 
            // In our current simple implementation, we just want to test sending from 2 to 1.
            
            let term = 5;
            let candidate_id = 99;

            // Node 2 broadcasts (should send to addr1)
            node2.broadcast_vote_request(term, candidate_id).await.expect("Failed to broadcast");

            // Node 1 should receive
            // We use timeout to avoid hanging if it fails
            let receive_future = node1.receive();
            match tokio::time::timeout(Duration::from_secs(1), receive_future).await {
                Ok(Ok(packet)) => {
                    match packet {
                        Packet::VoteRequest { term: t, candidate_id: c } => {
                            assert_eq!(t, term);
                            assert_eq!(c, candidate_id);
                        }
                        _ => panic!("Received wrong packet type"),
                    }
                }
                Ok(Err(e)) => panic!("Receive failed: {}", e),
                Err(_) => panic!("Receive timed out"),
            }
        }

        #[tokio::test]
        async fn test_dynamic_peers_update() {
             let node1 = UdpTransport::new("127.0.0.1:0", vec![]).await.expect("Failed to create node1");
             
             // Update peers
             let res = node1.update_peers(vec!["127.0.0.1:9999".to_string()]).await;
             assert!(res.is_ok());
        }
    }
}
