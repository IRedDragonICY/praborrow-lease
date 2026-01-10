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
    // Add AppendEntries/InstallSnapshot as needed
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
}

#[cfg(feature = "net")]
pub mod udp {
    use super::*;
    use std::net::{UdpSocket, SocketAddr};
    use std::sync::{Arc, Mutex};
    use std::io::ErrorKind;

    /// UDP-based network transport for consensus algorithms.
    pub struct UdpTransport {
        socket: Arc<UdpSocket>,
        peers: Vec<String>,
        config: NetworkConfig,
        /// Current backoff duration for supervisor restart
        current_backoff: Mutex<Duration>,
        /// Last failure time for backoff reset
        last_failure: Mutex<Option<std::time::Instant>>,
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
        ///
        /// # Arguments
        ///
        /// * `bind_addr` - The address to bind to (e.g., "0.0.0.0:8080")
        /// * `peers` - List of peer addresses to communicate with
        ///
        /// # Errors
        ///
        /// Returns an error if:
        /// - The bind address is invalid
        /// - The socket cannot be bound
        pub fn new(bind_addr: &str, peers: Vec<String>) -> Result<Self, String> {
            Self::with_config(bind_addr, peers, NetworkConfig::default())
        }

        /// Creates a new UDP transport with custom configuration.
        ///
        /// # Arguments
        ///
        /// * `bind_addr` - The address to bind to
        /// * `peers` - List of peer addresses
        /// * `config` - Network configuration
        ///
        /// # Errors
        ///
        /// Returns an error if the address is invalid or socket binding fails.
        pub fn with_config(
            bind_addr: &str,
            peers: Vec<String>,
            config: NetworkConfig,
        ) -> Result<Self, String> {
            // Validate address format first
            let _: SocketAddr = bind_addr.parse().map_err(|e| {
                format!(
                    "Invalid bind address '{}': {}. Expected format: 'IP:PORT' (e.g., '0.0.0.0:8080')",
                    bind_addr, e
                )
            })?;

            let socket = UdpSocket::bind(bind_addr).map_err(|e| {
                format!("Failed to bind UDP socket to '{}': {}", bind_addr, e)
            })?;

            // Set read timeout to prevent blocking forever
            socket.set_read_timeout(Some(config.read_timeout)).map_err(|e| {
                format!("Failed to set socket read timeout: {}", e)
            })?;

            tracing::info!(
                bind_addr = bind_addr,
                peer_count = peers.len(),
                buffer_size = config.buffer_size,
                read_timeout_ms = config.read_timeout.as_millis() as u64,
                "UDP transport initialized"
            );

            Ok(Self {
                socket: Arc::new(socket),
                peers,
                config,
                current_backoff: Mutex::new(INITIAL_BACKOFF),
                last_failure: Mutex::new(None),
            })
        }

        /// Calculates the next backoff duration using exponential backoff.
        fn calculate_backoff(&self) -> Duration {
            let mut backoff = self.current_backoff.lock().unwrap();
            let mut last_failure = self.last_failure.lock().unwrap();

            let now = std::time::Instant::now();

            // Reset backoff if enough time has passed since last failure
            if let Some(last) = *last_failure {
                if now.duration_since(last) > Duration::from_secs(60) {
                    *backoff = self.config.initial_backoff;
                }
            }

            *last_failure = Some(now);

            let current = *backoff;
            
            // Exponential backoff: double the duration, cap at max
            *backoff = std::cmp::min(
                self.config.max_backoff,
                current.saturating_mul(2),
            );

            current
        }

        /// Resets the backoff to initial value (called on successful receive)
        fn reset_backoff(&self) {
            let mut backoff = self.current_backoff.lock().unwrap();
            *backoff = self.config.initial_backoff;
        }
    }

    #[async_trait]
    impl ConsensusNetwork for UdpTransport {
        async fn broadcast_vote_request(&self, term: Term, candidate_id: NodeId) -> Result<(), String> {
            let packet = Packet::VoteRequest { term, candidate_id };
            let serialized = serde_json::to_vec(&packet).map_err(|e| {
                tracing::error!(error = %e, "Failed to serialize VoteRequest");
                e.to_string()
            })?;

            if serialized.len() > MAX_PACKET_SIZE {
                tracing::warn!(
                    size = serialized.len(),
                    max = MAX_PACKET_SIZE,
                    "VoteRequest packet exceeds maximum size"
                );
            }
            
            tracing::debug!(
                term = term,
                candidate_id = candidate_id,
                peer_count = self.peers.len(),
                "Broadcasting VoteRequest"
            );

            for peer in &self.peers {
                if let Err(e) = self.socket.send_to(&serialized, peer) {
                    tracing::warn!(
                        peer = peer,
                        error = %e,
                        "Failed to send VoteRequest to peer"
                    );
                }
            }
            Ok(())
        }

        async fn send_heartbeat(&self, leader_id: NodeId, term: Term) -> Result<(), String> {
            let packet = Packet::Heartbeat { leader_id, term };
            let serialized = serde_json::to_vec(&packet).map_err(|e| {
                tracing::error!(error = %e, "Failed to serialize Heartbeat");
                e.to_string()
            })?;
            
            tracing::trace!(
                term = term,
                leader_id = leader_id,
                "Sending heartbeat"
            );

            for peer in &self.peers {
                let _ = self.socket.send_to(&serialized, peer);
            }
            Ok(())
        }

        async fn receive(&self) -> Result<Packet, String> {
            loop {
                let socket_clone = self.socket.clone();
                let buffer_size = self.config.buffer_size;
                
                // Spawn a thread for blocking receive
                // Return type is Result<(Packet, SocketAddr), RecvError>
                let handle = std::thread::spawn(move || -> Result<(Packet, std::net::SocketAddr), RecvError> {
                    let mut buf = vec![0u8; buffer_size];
                    match socket_clone.recv_from(&mut buf) {
                        Ok((amt, src)) => {
                            if amt == buffer_size {
                                tracing::warn!(
                                    size = amt,
                                    source = %src,
                                    "Received packet may be truncated (filled entire buffer)"
                                );
                            }
                            let packet: Packet = serde_json::from_slice(&buf[..amt])
                                .map_err(|e| RecvError::Parse(e.to_string()))?;
                            Ok((packet, src))
                        }
                        Err(e) => Err(RecvError::Io(e)),
                    }
                });

                match handle.join() {
                    Ok(Ok((packet, src))) => {
                        self.reset_backoff();
                        tracing::trace!(source = %src, "Received packet");
                        return Ok(packet);
                    }
                    Ok(Err(RecvError::Io(e))) => {
                        // Handle specific error types gracefully
                        match e.kind() {
                            ErrorKind::WouldBlock | ErrorKind::TimedOut => {
                                // Timeout is expected, just continue the loop
                                continue;
                            }
                            _ => {
                                tracing::error!(
                                    error = %e,
                                    kind = ?e.kind(),
                                    "UDP receive error"
                                );
                                // For other errors, apply backoff before retry
                                let backoff = self.calculate_backoff();
                                std::thread::sleep(backoff);
                                continue;
                            }
                        }
                    }
                    Ok(Err(RecvError::Parse(msg))) => {
                        tracing::warn!(error = msg, "Failed to parse received packet");
                        continue;
                    }
                    Err(_) => {
                        // Thread panicked - apply exponential backoff
                        let backoff = self.calculate_backoff();
                        tracing::error!(
                            backoff_ms = backoff.as_millis() as u64,
                            "UDP receiver thread panicked, restarting with backoff"
                        );
                        std::thread::sleep(backoff);
                        continue;
                    }
                }
            }
        }
    }

    /// Error type for receive operations
    enum RecvError {
        Io(std::io::Error),
        Parse(String),
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

        #[test]
        fn test_invalid_bind_address() {
            let result = UdpTransport::new("not-an-address", vec![]);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("Invalid bind address"));
        }

        #[test]
        fn test_valid_bind_address() {
            // Use port 0 to let OS assign an available port
            let result = UdpTransport::new("127.0.0.1:0", vec![]);
            assert!(result.is_ok());
        }
    }
}
