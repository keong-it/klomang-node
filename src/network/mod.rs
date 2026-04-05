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

use std::sync::{Arc, RwLock};
use futures::StreamExt;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    gossipsub::{self, Behaviour as Gossipsub, Config as GossipsubConfig, MessageAuthenticity},
    identify::{Behaviour as Identify, Config as IdentifyConfig},
    identity::Keypair,
    kad::{Kademlia, store::MemoryStore},
    swarm::{NetworkBehaviour, Swarm, SwarmBuilder, SwarmEvent},
    PeerId, Transport,
};
use either::Either;
use void::Void;
use libp2p_quic::{Config as QuicConfig, GenTransport};
use crate::storage::db::StorageHandle;
use crate::state::ingestion::IngestionSender;
use crate::ingestion_guard::IngestionMessage;
use bincode;
use klomang_core::{BlockNode, Transaction};
use crate::state::KlomangStateManager;use crate::mempool::network::PeerManager;
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
                "/ip4/0.0.0.0/udp/3833/quic-v1".to_string(),
                "/ip6/::/udp/3833/quic-v1".to_string(),
            ],
            bootstrap_peers: vec![
                // Placeholder seed nodes - replace with real IPs when available
                // "/ip4/127.0.0.1/udp/3833/quic-v1/p2p/12D3KooWAbc123...".to_string(),
            ],
            max_connections: 50,
            connection_timeout: 30,
        }
    }
}

/// Network message types for P2P communication
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum NetworkMessage {
    /// Block propagation
    Block(BlockNode),
    /// Transaction propagation
    Transaction(Transaction),
}

/// Combined network behaviour for Klomang P2P networking
#[derive(NetworkBehaviour)]
pub struct KlomangNetworkBehaviour {
    /// Kademlia DHT for peer discovery
    kademlia: Kademlia<MemoryStore>,
    /// Gossipsub for efficient block and transaction propagation
    gossipsub: Gossipsub,
    /// Identify for exchanging node information
    identify: Identify,
}

/// Network manager for P2P operations
pub struct NetworkManager {
    /// The libp2p swarm
    swarm: Swarm<KlomangNetworkBehaviour>,
    /// Storage handle for persisting network state
    storage: StorageHandle,
    /// Configuration
    config: NetworkConfig,
    /// Connected peers
    connected_peers: HashSet<PeerId>,
    /// Local peer ID
    local_peer_id: PeerId,
    /// Ingestion sender for block processing
    ingestion_sender: IngestionSender,
    /// State manager for transaction validation
    state_manager: Arc<RwLock<KlomangStateManager>>,
    /// Peer manager for network intelligence
    peer_manager: Arc<RwLock<PeerManager>>,
}

impl NetworkManager {
    /// Create a new network manager
    pub async fn new(storage: StorageHandle, config: NetworkConfig, ingestion_sender: IngestionSender, state_manager: Arc<RwLock<KlomangStateManager>>) -> Result<Self, Box<dyn std::error::Error>> {
        // Load or generate Ed25519 keypair
        let keypair = Self::load_or_generate_keypair(&storage).await?;
        let local_peer_id = PeerId::from(keypair.public());

        log::info!("[NETWORK] Local PeerID: {}", local_peer_id);

        // Create QUIC transport
        let transport = Self::create_transport(keypair.clone())?;

        // Create Kademlia DHT
        let store = MemoryStore::new(local_peer_id);
        let mut kademlia = Kademlia::new(local_peer_id, store);

        // Create Gossipsub
        let gossipsub_config = GossipsubConfig::default();
        let mut gossipsub = Gossipsub::new(MessageAuthenticity::Signed(keypair.clone()), gossipsub_config)
            .map_err(|e| format!("Failed to create Gossipsub: {}", e))?;

        // Subscribe to topics
        let block_topic = gossipsub::IdentTopic::new("klomang/blocks");
        gossipsub.subscribe(&block_topic)?;
        let tx_topic = gossipsub::IdentTopic::new("klomang/transactions");
        gossipsub.subscribe(&tx_topic)?;

        // Create Identify
        let identify = Identify::new(IdentifyConfig::new("klomang/1.0.0".to_string(), keypair.public()));

        // Combine behaviours
        let behaviour = KlomangNetworkBehaviour {
            kademlia,
            gossipsub,
            identify,
        };

        // Create swarm
        let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build();

        // Listen on configured addresses
        for addr in &config.listen_addresses {
            swarm.listen_on(addr.parse()?)?;
            log::info!("[NETWORK] Listening on: {}", addr);
        }

        // Bootstrap Kademlia with known peers
        for peer_addr in &config.bootstrap_peers {
            if let Ok(addr) = peer_addr.parse::<libp2p::Multiaddr>() {
                swarm.dial(addr.clone())?;
                log::info!("[NETWORK] Dialing bootstrap peer: {}", peer_addr);
            }
        }

        // Start Kademlia bootstrap if we have bootstrap peers
        if !config.bootstrap_peers.is_empty() {
            swarm.behaviour_mut().kademlia.bootstrap()?;
        }

        Ok(NetworkManager {
            swarm,
            storage,
            config,
            connected_peers: HashSet::new(),
            local_peer_id,
            ingestion_sender,
            state_manager,
            peer_manager: Arc::new(RwLock::new(PeerManager::new())),
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

    /// Create QUIC transport
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

    /// Handle swarm events
    async fn handle_swarm_event(&mut self, event: SwarmEvent<KlomangNetworkBehaviourEvent, Either<Either<std::io::Error, Void>, std::io::Error>>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("[NETWORK] Listening on {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                log::info!("[NETWORK] Connected to peer: {}", peer_id);
                self.connected_peers.insert(peer_id);
                // Add to Kademlia
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, endpoint.get_remote_address().clone());
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                log::info!("[NETWORK] Disconnected from peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
            }
            SwarmEvent::Behaviour(event) => {
                self.handle_behaviour_event(event).await;
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

    /// Handle behaviour events
    async fn handle_behaviour_event(&mut self, event: KlomangNetworkBehaviourEvent) {
        match event {
            KlomangNetworkBehaviourEvent::Gossipsub(gossip_event) => {
                self.handle_gossipsub_event(gossip_event).await;
            }
            KlomangNetworkBehaviourEvent::Kademlia(kad_event) => {
                // Handle Kademlia events if needed
                log::debug!("[NETWORK] Kademlia event: {:?}", kad_event);
            }
            KlomangNetworkBehaviourEvent::Identify(identify_event) => {
                // Handle Identify events if needed
                log::debug!("[NETWORK] Identify event: {:?}", identify_event);
            }
        }
    }

    /// Handle Gossipsub events
    async fn handle_gossipsub_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message { message, .. } => {
                log::info!("[NETWORK] Received Gossipsub message on topic: {}", message.topic);
                // Try to deserialize as NetworkMessage
                match bincode::deserialize::<NetworkMessage>(&message.data) {
                    Ok(NetworkMessage::Block(block)) => {
                        log::info!("[NETWORK] Deserialized block {} from peer {:?}", block.header.id, message.source);
                        if let Err(e) = self.ingestion_sender.send(IngestionMessage::Block(block)).await {
                            log::error!("[NETWORK] Failed to send block to ingestion queue: {}", e);
                        } else {
                            log::info!("[NETWORK] Block queued for processing");
                        }
                    }
                    Ok(NetworkMessage::Transaction(tx)) => {
                        log::info!("[NETWORK] Deserialized transaction from peer {:?}", message.source);

                        // Check peer reputation and rate limits
                        if let Some(peer_id) = message.source {
                            let mut peer_manager = self.peer_manager.write().unwrap();
                            if peer_manager.is_peer_banned(&peer_id) {
                                log::warn!("[NETWORK] Ignoring transaction from banned peer {}", peer_id);
                                return;
                            }

                            match peer_manager.handle_incoming_transaction(peer_id, tx.clone()) {
                                Ok(()) => {
                                    // Add to mempool
                                    match self.state_manager.write().map_err(|_| "State manager lock poisoned".to_string()) {
                                        Ok(mut sm) => {
                                            match sm.add_transaction(tx.clone()) {
                                                Ok(tx_hash) => {
                                                    log::info!("[NETWORK] Transaction {} added to mempool", tx_hash);
                                                    // Gossip to other peers
                                                    if let Err(e) = self.gossip_transaction(&tx).await {
                                                        log::warn!("[NETWORK] Failed to gossip transaction: {}", e);
                                                    }
                                                }
                                                Err(e) => {
                                                    log::warn!("[NETWORK] Transaction validation failed: {}", e);
                                                    peer_manager.penalize_peer(&peer_id, "invalid transaction");
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::error!("[NETWORK] Failed to acquire state manager lock: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::warn!("[NETWORK] Peer {} transaction rejected: {}", peer_id, e);
                                }
                            }
                        } else {
                            log::warn!("[NETWORK] Received transaction without peer source");
                        }
                    }
                    Err(e) => {
                        log::warn!("[NETWORK] Failed to deserialize message as NetworkMessage: {}", e);
                    }
                }
            }
            gossipsub::Event::Subscribed { peer_id, topic } => {
                log::info!("[NETWORK] Peer {} subscribed to topic {}", peer_id, topic);
            }
            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                log::info!("[NETWORK] Peer {} unsubscribed from topic {}", peer_id, topic);
            }
            _ => {}
        }
    }

    /// Gossip a transaction to the network
    async fn gossip_transaction(&mut self, tx: &Transaction) -> Result<(), Box<dyn std::error::Error>> {
        let message = NetworkMessage::Transaction(tx.clone());
        let data = bincode::serialize(&message)?;
        let topic = gossipsub::IdentTopic::new("klomang/transactions");
        self.swarm.behaviour_mut().gossipsub.publish(topic, data)?;
        Ok(())
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

    /// Run the network event loop
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
            }
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

    // #[tokio::test]
    // async fn test_network_manager_creation() {
    //     // Create temporary storage for testing
    //     let temp_dir = tempfile::tempdir().unwrap();
    //     let storage = KlomangStorage::open(temp_dir.path()).unwrap();
    //     let storage_handle = Arc::new(RwLock::new(storage));

    //     let config = NetworkConfig::default();
    //     let manager = NetworkManager::new(storage_handle, config).await.unwrap();

    //     assert!(!manager.local_peer_id().to_string().is_empty());
    // }

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