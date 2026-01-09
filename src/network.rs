use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::raft::{Term, NodeId};

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
    use std::net::UdpSocket;
    use std::sync::{Arc, Mutex};

    pub struct UdpTransport {
        socket: Arc<UdpSocket>,
        peers: Vec<String>, // List of peer addresses
    }

    impl UdpTransport {
        pub fn new(bind_addr: &str, peers: Vec<String>) -> Result<Self, String> {
            let socket = UdpSocket::bind(bind_addr).map_err(|e| e.to_string())?;
            // Optional: socket.set_nonblocking(true)?; 
            // For this basic impl, we keep it blocking as we don't have an async runtime to poll properly without one.
            Ok(Self {
                socket: Arc::new(socket),
                peers,
            })
        }
    }

    #[async_trait]
    impl ConsensusNetwork for UdpTransport {
        async fn broadcast_vote_request(&self, term: Term, candidate_id: NodeId) -> Result<(), String> {
            let packet = Packet::VoteRequest { term, candidate_id };
            let serialized = serde_json::to_vec(&packet).map_err(|e| e.to_string())?;
            
            for peer in &self.peers {
                // Ignore errors to individual peers
                let _ = self.socket.send_to(&serialized, peer);
            }
            Ok(())
        }

        async fn send_heartbeat(&self, leader_id: NodeId, term: Term) -> Result<(), String> {
            let packet = Packet::Heartbeat { leader_id, term };
            let serialized = serde_json::to_vec(&packet).map_err(|e| e.to_string())?;
            
            for peer in &self.peers {
                let _ = self.socket.send_to(&serialized, peer);
            }
            Ok(())
        }

        async fn receive(&self) -> Result<Packet, String> {
            let socket = self.socket.clone();
            // This blocks the thread, which is not ideal for async, but fulfills the interface
            // using standard UdpSocket without introducing tokio.
            // In a real async system, we'd use use tokio::net::UdpSocket or spawn_blocking.
            let (packet, _) = std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let (amt, _src) = socket.recv_from(&mut buf).map_err(|e| e.to_string())?;
                let packet: Packet = serde_json::from_slice(&buf[..amt]).map_err(|e| e.to_string())?;
                Ok((packet, _src))
            }).join().unwrap()?; // Unwrap thread panic, propagate Result
            
            Ok(packet)
        }
    }
}
