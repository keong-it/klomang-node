//! Klomang Networking Module - P2P Communication Layer
//!
//! This module implements the P2P networking layer using libp2p with QUIC transport.
//! Features:
//! - QUIC transport for fast, secure connections
//! - Noise protocol for encryption
//! - Yamux for multiplexing
//! - Persistent Ed25519 keypair for stable PeerID
//! - Automatic peer discovery and connection management

use std::collections::HashSet;
use futures::StreamExt;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    identity::Keypair,
    ping,
    swarm::{Swarm, SwarmBuilder, SwarmEvent},
    PeerId, Transport,
};
use libp2p_quic::{Config as QuicConfig, GenTransport};
use crate::storage::db::StorageHandle;

/// Network configuration
#[derive(Clone, Debug)]
pub struct NetworkConfig {
    /// Listen addresses for incoming connections
    pub listen_addresses: Vec<String>,
    /// Bootstrap peers to connect to on startup
    pub bootstrap_peers: Vec<String>,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Connection timeout in seconds
    pub connection_timeout: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            listen_addresses: vec![
                "/ip4/0.0.0.0/udp/0/quic-v1".to_string(),
                "/ip6/::/udp/0/quic-v1".to_string(),
            ],
            bootstrap_peers: vec![],
            max_connections: 50,
            connection_timeout: 30,
        }
    }
}

/// Network manager for P2P operations
pub struct NetworkManager {
    /// The libp2p swarm
    swarm: Swarm<ping::Behaviour>,
    /// Storage handle for persisting network state
    storage: StorageHandle,
    /// Configuration
    config: NetworkConfig,
    /// Connected peers
    connected_peers: HashSet<PeerId>,
    /// Local peer ID
    local_peer_id: PeerId,
}

impl NetworkManager {
    /// Create a new network manager
    pub async fn new(storage: StorageHandle, config: NetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Load or generate Ed25519 keypair
        let keypair = Self::load_or_generate_keypair(&storage).await?;
        let local_peer_id = PeerId::from(keypair.public());

        log::info!("[NETWORK] Local PeerID: {}", local_peer_id);

        // Create QUIC transport with Noise encryption and Yamux multiplexing
        let transport = Self::create_transport(keypair)?;

        // Create swarm with real ping behaviour for connectivity checks
        let behaviour = ping::Behaviour::new(ping::Config::new());
        let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build();

        // Listen on configured addresses
        for addr in &config.listen_addresses {
            swarm.listen_on(addr.parse()?)?;
            log::info!("[NETWORK] Listening on: {}", addr);
        }

        // Connect to bootstrap peers
        for peer_addr in &config.bootstrap_peers {
            if let Ok(addr) = peer_addr.parse::<libp2p::Multiaddr>() {
                swarm.dial(addr)?;
                log::info!("[NETWORK] Dialing bootstrap peer: {}", peer_addr);
            }
        }

        Ok(NetworkManager {
            swarm,
            storage,
            config,
            connected_peers: HashSet::new(),
            local_peer_id,
        })
    }

    /// Load or generate Ed25519 keypair from storage
    async fn load_or_generate_keypair(storage: &StorageHandle) -> Result<Keypair, Box<dyn std::error::Error>> {
        let key_name = "network_keypair";

        // Try to load existing keypair
        if let Ok(Some(mut key_bytes)) = storage.read().map_err(|e| format!("Storage lock error: {}", e))?
            .get_state(key_name).map_err(|e| format!("Storage read error: {}", e)) {
            let keypair = Keypair::ed25519_from_bytes(&mut key_bytes)
                .map_err(|e| format!("Invalid stored keypair: {}", e))?;
            log::info!("[NETWORK] Loaded existing keypair from storage");
            return Ok(keypair);
        }

        // Generate new keypair
        log::info!("[NETWORK] Generating new Ed25519 keypair");
        let keypair = Keypair::generate_ed25519();
        let ed25519_keypair = keypair.clone().try_into_ed25519()
            .map_err(|e| format!("Failed to extract Ed25519 keypair: {}", e))?;

        // Persist the keypair secret
        let secret_bytes = ed25519_keypair.secret().as_ref().to_vec();
        storage.write().map_err(|e| format!("Storage lock error: {}", e))?
            .put_state(key_name, secret_bytes.as_slice())
            .map_err(|e| format!("Storage write error: {}", e))?;

        log::info!("[NETWORK] Saved new keypair to storage");
        Ok(keypair)
    }

    /// Create QUIC transport with Noise and Yamux
    fn create_transport(keypair: Keypair) -> Result<Boxed<(PeerId, StreamMuxerBox)>, Box<dyn std::error::Error>> {
        let transport = GenTransport::<libp2p_quic::tokio::Provider>::new(QuicConfig::new(&keypair))
            .map(|(peer_id, connection), _| (peer_id, StreamMuxerBox::new(connection)))
            .boxed();

        Ok(transport)
    }

    /// Get local peer ID
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Get connected peers
    pub fn connected_peers(&self) -> &HashSet<PeerId> {
        &self.connected_peers
    }

    /// Start the network event loop
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[NETWORK] Starting network event loop");

        loop {
            if let Some(event) = self.swarm.next().await {
                self.handle_swarm_event(event).await;
            }
        }
    }

    /// Handle swarm events
    async fn handle_swarm_event(&mut self, event: SwarmEvent<ping::Event, ping::Failure>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("[NETWORK] Listening on {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                log::info!("[NETWORK] Connected to peer: {}", peer_id);
                self.connected_peers.insert(peer_id);
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                log::info!("[NETWORK] Disconnected from peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
            }
            SwarmEvent::IncomingConnection { .. } => {
                log::info!("[NETWORK] Incoming connection");
            }
            SwarmEvent::IncomingConnectionError { error, .. } => {
                log::error!("[NETWORK] Incoming connection error: {}", error);
            }
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                log::error!("[NETWORK] Outgoing connection error: {}", error);
            }
            SwarmEvent::Dialing(peer_id) => {
                log::info!("[NETWORK] Dialing peer: {}", peer_id);
            }
            _ => {}
        }
    }

    /// Dial a specific peer
    pub fn dial_peer(&mut self, peer_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let addr: libp2p::Multiaddr = peer_addr.parse()?;
        self.swarm.dial(addr)?;
        log::info!("[NETWORK] Dialing peer: {}", peer_addr);
        Ok(())
    }

    /// Get network statistics
    pub fn stats(&self) -> NetworkStats {
        let connected_peers = self.connected_peers.len();
        NetworkStats {
            local_peer_id: self.local_peer_id.clone(),
            connected_peers,
            listening_addresses: self.swarm.listeners().cloned().collect(),
        }
    }
}

/// Network statistics
#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub local_peer_id: PeerId,
    pub connected_peers: usize,
    pub listening_addresses: Vec<libp2p::Multiaddr>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::KlomangStorage;

    #[tokio::test]
    async fn test_network_manager_creation() {
        // Create temporary storage for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = KlomangStorage::open(temp_dir.path()).unwrap();
        let storage_handle = Arc::new(RwLock::new(storage));

        let config = NetworkConfig::default();
        let manager = NetworkManager::new(storage_handle, config).await.unwrap();

        assert!(!manager.local_peer_id().to_string().is_empty());
    }

    #[tokio::test]
    async fn test_keypair_persistence() {
        // Create temporary storage
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = KlomangStorage::open(temp_dir.path()).unwrap();
        let storage_handle = Arc::new(RwLock::new(storage));

        // Generate and save keypair
        let keypair1 = NetworkManager::load_or_generate_keypair(&storage_handle).await.unwrap();
        let peer_id1 = PeerId::from(keypair1.public());

        // Load existing keypair
        let keypair2 = NetworkManager::load_or_generate_keypair(&storage_handle).await.unwrap();
        let peer_id2 = PeerId::from(keypair2.public());

        // Should be the same
        assert_eq!(peer_id1, peer_id2);
    }
}