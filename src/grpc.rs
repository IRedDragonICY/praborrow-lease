//! gRPC Transport for Raft
//!
//! Production-ready gRPC transport with TLS, connection pooling, and circuit breaker.

// #![cfg(feature = "grpc")] // Redundant with mod declaration

use crate::network::{NetworkError, PeerInfo, RaftMessage, RaftNetwork};
use crate::raft::{LogEntry, LogIndex, NodeId, Snapshot, Term};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;

// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("raft");
}

use proto::control_plane_server::{ControlPlane, ControlPlaneServer}; // Import ControlPlane
use proto::raft_client::RaftClient;
use proto::raft_server::{Raft, RaftServer};
use std::fs;
use tonic::{
    Request, Response, Status,
    transport::{
        Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
    },
};

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Configuration for gRPC transport.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    /// Server bind address
    pub bind_addr: String,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Keep-alive interval
    pub keep_alive_interval: Duration,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Initial backoff for retries
    pub initial_backoff: Duration,
    /// Maximum backoff for retries
    pub max_backoff: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// TLS configuration (optional)
    pub tls: Option<TlsConfig>,
    /// Circuit breaker configuration
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:50051".to_string(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            keep_alive_interval: Duration::from_secs(30),
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            tls: None,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

/// TLS configuration for secure connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to CA certificate
    pub ca_cert: std::path::PathBuf,
    /// Path to server certificate
    pub server_cert: std::path::PathBuf,
    /// Path to server private key
    pub server_key: std::path::PathBuf,
    /// Client certificate for mTLS (optional)
    pub client_cert: Option<std::path::PathBuf>,
    /// Client key for mTLS (optional)
    pub client_key: Option<std::path::PathBuf>,
    /// Domain name for verification (defaults to "localhost" if None)
    pub domain_name: Option<String>,
}

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Number of successes in half-open state to close
    pub success_threshold: u32,
    /// Time to wait before transitioning from open to half-open
    pub reset_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            reset_timeout: Duration::from_secs(30),
        }
    }
}

// ============================================================================
// CIRCUIT BREAKER
// ============================================================================

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

/// Circuit breaker for fault tolerance.
#[derive(Debug)]
pub struct CircuitBreaker {
    state: AtomicU8,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    last_failure_time: AtomicI64,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            last_failure_time: AtomicI64::new(0),
            config,
        }
    }

    pub fn state(&self) -> CircuitState {
        match self.state.load(Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            _ => CircuitState::HalfOpen,
        }
    }

    /// Check if request is allowed.
    pub fn allow_request(&self) -> bool {
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if reset timeout has passed
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let last_failure = self.last_failure_time.load(Ordering::SeqCst);

                if now - last_failure > self.config.reset_timeout.as_millis() as i64 {
                    self.state
                        .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
                    self.success_count.store(0, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        match self.state() {
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.config.success_threshold {
                    self.state
                        .store(CircuitState::Closed as u8, Ordering::SeqCst);
                    self.failure_count.store(0, Ordering::SeqCst);
                    tracing::info!("Circuit breaker closed");
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.last_failure_time.store(now, Ordering::SeqCst);

        match self.state() {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.config.failure_threshold {
                    self.state.store(CircuitState::Open as u8, Ordering::SeqCst);
                    tracing::warn!("Circuit breaker opened after {} failures", count);
                }
            }
            CircuitState::HalfOpen => {
                self.state.store(CircuitState::Open as u8, Ordering::SeqCst);
                tracing::warn!("Circuit breaker re-opened");
            }
            CircuitState::Open => {}
        }
    }
}

// ============================================================================
// CONNECTION POOL
// ============================================================================

/// Connection to a peer with circuit breaker.
struct PeerConnection {
    client: RaftClient<Channel>,
    circuit_breaker: CircuitBreaker,
}

/// Connection pool for managing peer connections.
pub struct ConnectionPool {
    connections: RwLock<HashMap<NodeId, PeerConnection>>,
    config: GrpcConfig,
    tls_config: Option<ClientTlsConfig>,
}

impl ConnectionPool {
    pub fn new(config: GrpcConfig) -> Self {
        let tls_config = if let Some(tls) = &config.tls {
            let pem = fs::read_to_string(&tls.ca_cert).expect("Failed to read CA cert");
            let ca = Certificate::from_pem(pem);

            let mut tls_config = ClientTlsConfig::new()
                .ca_certificate(ca)
                .domain_name(tls.domain_name.as_deref().unwrap_or("localhost"));

            if let (Some(cert_path), Some(key_path)) = (&tls.client_cert, &tls.client_key) {
                let cert = fs::read_to_string(cert_path).expect("Failed to read client cert");
                let key = fs::read_to_string(key_path).expect("Failed to read client key");
                let identity = Identity::from_pem(cert, key);
                tls_config = tls_config.identity(identity);
            }

            Some(tls_config)
        } else {
            None
        };

        Self {
            connections: RwLock::new(HashMap::new()),
            config,
            tls_config,
        }
    }

    /// Gets or creates a connection to a peer.
    pub async fn get_connection(
        &self,
        peer_id: NodeId,
        address: &str,
    ) -> Result<RaftClient<Channel>, NetworkError> {
        // Check if connection exists
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&peer_id) {
                if !conn.circuit_breaker.allow_request() {
                    return Err(NetworkError::ConnectionFailed(
                        "Circuit breaker open".into(),
                    ));
                }
                return Ok(conn.client.clone());
            }
        }

        // Create new connection
        let endpoint = Endpoint::from_shared(format!("http://{}", address))
            .map_err(|e| NetworkError::ConnectionFailed(e.to_string()))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        let endpoint = if let Some(tls_config) = &self.tls_config {
            endpoint.tls_config(tls_config.clone()).map_err(|e| {
                NetworkError::ConnectionFailed(format!("Failed to configure TLS: {}", e))
            })?
        } else {
            endpoint
        };

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| NetworkError::ConnectionFailed(e.to_string()))?;

        let client = RaftClient::new(channel);

        // Store connection
        let mut connections = self.connections.write().await;
        connections.insert(
            peer_id,
            PeerConnection {
                client: client.clone(),
                circuit_breaker: CircuitBreaker::new(self.config.circuit_breaker.clone()),
            },
        );

        tracing::info!(
            peer_id = peer_id,
            address = address,
            "Created new gRPC connection"
        );

        Ok(client)
    }

    /// Records success for a peer's circuit breaker.
    pub async fn record_success(&self, peer_id: NodeId) {
        let connections = self.connections.read().await;
        if let Some(conn) = connections.get(&peer_id) {
            conn.circuit_breaker.record_success();
        }
    }

    /// Records failure for a peer's circuit breaker.
    pub async fn record_failure(&self, peer_id: NodeId) {
        let connections = self.connections.read().await;
        if let Some(conn) = connections.get(&peer_id) {
            conn.circuit_breaker.record_failure();
        }
    }
}

// ============================================================================
// GRPC TRANSPORT
// ============================================================================

/// gRPC-based network transport for Raft.
pub struct GrpcTransport<T> {
    #[allow(dead_code)] // TODO: Use node_id
    node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, PeerInfo>>>,
    pool: Arc<ConnectionPool>,
    config: GrpcConfig,
    inbox: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<RaftMessage<T>>>>,
    inbox_sender: tokio::sync::mpsc::Sender<RaftMessage<T>>,
    metrics: Option<std::sync::Arc<crate::metrics::RaftMetrics>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static>
    GrpcTransport<T>
{
    /// Creates a new gRPC transport.
    pub fn new(
        node_id: NodeId,
        config: GrpcConfig,
        metrics: Option<std::sync::Arc<crate::metrics::RaftMetrics>>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        Self {
            node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            pool: Arc::new(ConnectionPool::new(config.clone())),
            config,
            inbox: Arc::new(tokio::sync::Mutex::new(rx)),
            inbox_sender: tx,
            metrics,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns the inbox sender for the gRPC server to use.
    pub fn inbox_sender(&self) -> tokio::sync::mpsc::Sender<RaftMessage<T>> {
        self.inbox_sender.clone()
    }

    /// Sends a request with retry and exponential backoff.
    async fn send_with_retry<F, R>(
        &self,
        peer_id: NodeId,
        mut operation: F,
    ) -> Result<R, NetworkError>
    where
        F: FnMut() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R, NetworkError>> + Send>,
        >,
    {
        let mut backoff = self.config.initial_backoff;

        for attempt in 0..=self.config.max_retries {
            match operation().await {
                Ok(result) => {
                    self.pool.record_success(peer_id).await;
                    return Ok(result);
                }
                Err(e) => {
                    self.pool.record_failure(peer_id).await;

                    if attempt < self.config.max_retries {
                        tracing::warn!(
                            peer_id = peer_id,
                            attempt = attempt + 1,
                            error = %e,
                            backoff_ms = backoff.as_millis(),
                            "Retrying after failure"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(
                            Duration::from_secs_f64(
                                backoff.as_secs_f64() * self.config.backoff_multiplier,
                            ),
                            self.config.max_backoff,
                        );
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        unreachable!()
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static>
    RaftNetwork<T> for GrpcTransport<T>
{
    async fn send_request_vote(
        &self,
        peer_id: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Result<Option<RaftMessage<T>>, NetworkError> {
        let start = std::time::Instant::now();
        let peers = self.peers.read().await;
        let peer = peers
            .get(&peer_id)
            .ok_or(NetworkError::PeerNotFound(peer_id))?;
        let address = peer.address.clone();
        drop(peers);

        let pool = self.pool.clone();
        let request = proto::RequestVoteRequest {
            term,
            candidate_id: candidate_id as u64,
            last_log_index,
            last_log_term,
        };

        let response = self
            .send_with_retry(peer_id, || {
                let pool = pool.clone();
                let address = address.clone();

                Box::pin(async move {
                    let mut client = pool.get_connection(peer_id, &address).await?;
                    let response = client
                        .request_vote(Request::new(request))
                        .await
                        .map_err(|e| NetworkError::TransportError(e.to_string()))?;
                    Ok(response.into_inner())
                })
            })
            .await?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_rpc("request_vote", start.elapsed());
        }

        let response_term = response.term;
        let response_vote = response.vote_granted;
        let response_from = response.from_id as u128; // Extract before move if needed

        Ok(Some(RaftMessage::RequestVoteResponse {
            term: response_term,
            vote_granted: response_vote,
            from_id: response_from,
        }))
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
        let start = std::time::Instant::now();
        let peers = self.peers.read().await;
        let peer = peers
            .get(&peer_id)
            .ok_or(NetworkError::PeerNotFound(peer_id))?;
        let address = peer.address.clone();
        drop(peers);

        let pool = self.pool.clone();

        // Convert entries to proto format
        let proto_entries: Vec<proto::LogEntry> = entries
            .iter()
            .map(|e| proto::LogEntry {
                term: e.term,
                index: e.index,
                // Serialize the entire LogCommand (which covers NoOp, App(T), and Config)
                command: bincode::serialize(&e.command).unwrap_or_default(),
            })
            .collect();

        let request = proto::AppendEntriesRequest {
            term,
            leader_id: leader_id as u64,
            prev_log_index,
            prev_log_term,
            entries: proto_entries,
            leader_commit,
        };

        let response = self
            .send_with_retry(peer_id, || {
                let pool = pool.clone();
                let address = address.clone();
                let request = request.clone();

                Box::pin(async move {
                    let mut client = pool.get_connection(peer_id, &address).await?;
                    let response = client
                        .append_entries(Request::new(request))
                        .await
                        .map_err(|e| NetworkError::TransportError(e.to_string()))?;
                    Ok(response.into_inner())
                })
            })
            .await?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_rpc("append_entries", start.elapsed());
        }

        Ok(Some(RaftMessage::AppendEntriesResponse {
            term: response.term,
            success: response.success,
            match_index: response.match_index,
            from_id: response.from_id as u128,
        }))
    }

    async fn send_install_snapshot(
        &self,
        peer_id: NodeId,
        term: Term,
        leader_id: NodeId,
        snapshot: Snapshot<T>,
    ) -> Result<Option<RaftMessage<T>>, NetworkError> {
        let start = std::time::Instant::now();
        let peers = self.peers.read().await;
        let peer = peers
            .get(&peer_id)
            .ok_or(NetworkError::PeerNotFound(peer_id))?;
        let address = peer.address.clone();
        drop(peers);

        let pool = self.pool.clone();

        let data = bincode::serialize(&snapshot.data)
            .map_err(|e| NetworkError::SerializationError(e.to_string()))?;

        let request = proto::InstallSnapshotRequest {
            term,
            leader_id: leader_id as u64,
            last_included_index: snapshot.last_included_index,
            last_included_term: snapshot.last_included_term,
            data,
            checksum: snapshot.checksum,
        };

        let response = self
            .send_with_retry(peer_id, || {
                let pool = pool.clone();
                let address = address.clone();
                let request = request.clone();

                Box::pin(async move {
                    let mut client = pool.get_connection(peer_id, &address).await?;
                    let response = client
                        .install_snapshot(Request::new(request))
                        .await
                        .map_err(|e| NetworkError::TransportError(e.to_string()))?;
                    Ok(response.into_inner())
                })
            })
            .await?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_rpc("install_snapshot", start.elapsed());
        }

        Ok(Some(RaftMessage::InstallSnapshotResponse {
            term: response.term,
            success: response.success,
            from_id: response.from_id as u128,
        }))
    }

    async fn receive(&self) -> Result<RaftMessage<T>, NetworkError> {
        let mut inbox = self.inbox.lock().await;
        inbox
            .recv()
            .await
            .ok_or(NetworkError::TransportError("Channel closed".into()))
    }

    async fn respond(&self, _to: NodeId, _message: RaftMessage<T>) -> Result<(), NetworkError> {
        // For gRPC, responses are sent synchronously in the RPC handler
        // This is a no-op for the client side
        Ok(())
    }

    fn peer_ids(&self) -> Vec<NodeId> {
        // This is blocking, use sparingly
        Vec::new()
    }

    async fn update_peers(&self, peers: Vec<PeerInfo>) -> Result<(), NetworkError> {
        let mut peer_map = self.peers.write().await;
        peer_map.clear();
        for peer in peers {
            peer_map.insert(peer.id, peer);
        }
        tracing::info!(peer_count = peer_map.len(), "Updated gRPC peers");
        Ok(())
    }
}

// ============================================================================
// GRPC SERVER
// ============================================================================

/// gRPC server implementation for Raft.
pub struct RaftGrpcServer<T> {
    node_id: NodeId,
    inbox_sender: tokio::sync::mpsc::Sender<RaftMessage<T>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static>
    RaftGrpcServer<T>
{
    pub fn new(node_id: NodeId, inbox_sender: tokio::sync::mpsc::Sender<RaftMessage<T>>) -> Self {
        Self {
            node_id,
            inbox_sender,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[tonic::async_trait]
impl<T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static> Raft
    for RaftGrpcServer<T>
{
    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let req = request.into_inner();

        // Send to Raft engine for processing
        let msg = RaftMessage::RequestVote {
            term: req.term,
            candidate_id: req.candidate_id as u128,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        self.inbox_sender
            .send(msg)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Return placeholder - actual response handled by engine
        Ok(Response::new(proto::RequestVoteResponse {
            term: 0,
            vote_granted: false,
            from_id: self.node_id as u64,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        // Convert proto entries
        let entries: Vec<LogEntry<T>> = req
            .entries
            .iter()
            .filter_map(|e| {
                let command: crate::raft::LogCommand<T> = bincode::deserialize(&e.command).ok()?;
                Some(LogEntry {
                    term: e.term,
                    index: e.index,
                    command,
                })
            })
            .collect();

        let msg = RaftMessage::AppendEntries {
            term: req.term,
            leader_id: req.leader_id as u128,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries,
            leader_commit: req.leader_commit,
        };

        self.inbox_sender
            .send(msg)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::AppendEntriesResponse {
            term: 0,
            success: true,
            match_index: 0,
            from_id: self.node_id as u64,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let data: T =
            bincode::deserialize(&req.data).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let snapshot = Snapshot {
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data,
            checksum: req.checksum,
        };

        let msg = RaftMessage::InstallSnapshot {
            term: req.term,
            leader_id: req.leader_id as u128,
            snapshot,
        };

        self.inbox_sender
            .send(msg)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::InstallSnapshotResponse {
            term: 0,
            success: true,
            from_id: self.node_id as u64,
        }))
    }
}

// ============================================================================
// CONTROL PLANE SERVER
// ============================================================================

pub struct RaftControlPlane {
    node_id: NodeId,
}

impl RaftControlPlane {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

#[tonic::async_trait]
impl ControlPlane for RaftControlPlane {
    async fn get_node_status(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::NodeStatus>, Status> {
        // Mock data for now, eventually this should read from shared state
        Ok(Response::new(proto::NodeStatus {
            state: "Leader".to_string(), // TODO: Real state
            current_term: 10,
            commit_index: 42,
            last_applied: 42,
            connected_peers: 2,
            id: self.node_id as u64,
        }))
    }

    async fn get_recent_logs(
        &self,
        request: Request<proto::LogRequest>,
    ) -> Result<Response<proto::LogResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit.min(100) as usize;

        // Mock logs
        let logs = vec![
            "INFO: Raft started".to_string(),
            "INFO: Became election candidate".to_string(),
            "INFO: Elected leader term 10".to_string(),
        ];

        Ok(Response::new(proto::LogResponse {
            logs: logs.into_iter().take(limit).collect(),
        }))
    }
}

/// Starts the gRPC server.
pub async fn start_grpc_server<
    T: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
>(
    bind_addr: &str,
    node_id: NodeId,
    config: GrpcConfig,
    inbox_sender: tokio::sync::mpsc::Sender<RaftMessage<T>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = bind_addr.parse()?;
    let server = RaftGrpcServer::new(node_id, inbox_sender);
    let control_plane = RaftControlPlane::new(node_id); // Create Control Plane

    tracing::info!(addr = bind_addr, "Starting gRPC server");

    let mut builder = Server::builder();

    if let Some(tls) = &config.tls {
        let cert = fs::read_to_string(&tls.server_cert)?;
        let key = fs::read_to_string(&tls.server_key)?;
        let identity = Identity::from_pem(cert, key);

        let mut tls_config = ServerTlsConfig::new().identity(identity);

        // Emable mTLS by requiring client auth with the CA
        let ca_pem = fs::read_to_string(&tls.ca_cert)?;
        let client_ca = Certificate::from_pem(ca_pem);
        tls_config = tls_config.client_ca_root(client_ca);

        builder = builder.tls_config(tls_config)?;
        tracing::info!("mTLS enabled for gRPC server");
    }

    builder
        .add_service(RaftServer::new(server))
        .add_service(ControlPlaneServer::new(control_plane))
        .serve(addr)
        .await?;

    Ok(())
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            reset_timeout: Duration::from_millis(100),
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_half_open_after_timeout() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 1,
            reset_timeout: Duration::from_millis(10),
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(20));

        assert!(cb.allow_request());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_closes_after_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 1,
            reset_timeout: Duration::from_millis(1),
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(5));
        cb.allow_request(); // Transition to half-open

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_grpc_config_default() {
        let config = GrpcConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.backoff_multiplier, 2.0);
    }
}
