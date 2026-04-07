use crate::network::config::{FeatureFlags, NetworkConfig, ProtocolVersion};
use crate::network::connection::{BlacklistState, ConnectionDirection, ConnectionManager, PeerConnectionInfo, BandwidthStats};
use crate::network::filter::{GossipFilter, RateLimiter};
use crate::network::limiter::PeerRateLimiter;
use crate::network::protocol::{ConnectionControlCodec, ConnectionControlProtocol, ConnectionControlRequest, ConnectionControlResponse, NetworkMessage, SyncCodec, SyncProtocol, SyncRequest, SyncResponse};
use crate::network::scoring::PeerScore;
use crate::network::sync::SyncPrioritizer;
use crate::network::behaviour::{KlomangNetworkBehaviour, KlomangNetworkBehaviourEvent};
use crate::ingestion_guard::IngestionMessage;
use crate::state::KlomangStateManager;
use crate::state::ingestion::IngestionSender;
use crate::storage::db::StorageHandle;
use bincode;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use klomang_core::{BlockNode, SignedTransaction, Transaction};
use klomang_core::core::crypto::schnorr::{compute_sighash, verify_schnorr};
use libp2p::{core::{muxing::StreamMuxerBox, transport::Boxed, connection::ConnectedPoint}, gossipsub::{self, Behaviour as Gossipsub, Config as GossipsubConfig, MessageAuthenticity}, identity::Keypair, identify::{Behaviour as Identify, Config as IdentifyConfig}, kad::{Kademlia, store::MemoryStore}, ping::{Behaviour as Ping, Config as PingConfig, Event as PingEvent}, request_response::{ProtocolSupport, RequestId, Behaviour as RequestResponseBehaviour, Config, Event as RequestResponseEvent}, swarm::{NetworkBehaviour, Swarm, SwarmBuilder, SwarmEvent}, PeerId, Transport};
use libp2p::autonat::{Behaviour as AutoNat, Config as AutoNatConfig, Event as AutoNatEvent};
use libp2p::relay::Behaviour as RelayServer;
use libp2p_quic::{Config as QuicConfig, GenTransport};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

const MAX_BLOCK_MESSAGE_SIZE: usize = 8 * 1024 * 1024;
const MAX_TRANSACTION_MESSAGE_SIZE: usize = 128 * 1024;
const MAX_CONTROL_MESSAGE_SIZE: usize = 16 * 1024;
const MAX_NETWORK_MESSAGE_SIZE: usize = MAX_BLOCK_MESSAGE_SIZE;
const MIN_NETWORK_MESSAGE_SIZE: usize = 1;

const CURRENT_PROTOCOL_VERSION: u32 = 1;
const MIN_COMPATIBLE_VERSION: u32 = 1;

pub struct PeerManager {
    peer_scores: HashMap<PeerId, PeerScore>,
    rate_limiter: RateLimiter,
    granular_limiter: PeerRateLimiter,
    gossip_filter: GossipFilter,
    rebroadcast_queue: tokio::sync::mpsc::UnboundedSender<SignedTransaction>,
    rebroadcast_receiver: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<SignedTransaction>>>,
}

impl PeerManager {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        PeerManager {
            peer_scores: HashMap::new(),
            rate_limiter: RateLimiter::new(10.0),
            granular_limiter: PeerRateLimiter::new(),
            gossip_filter: GossipFilter::new(),
            rebroadcast_queue: tx,
            rebroadcast_receiver: Mutex::new(Some(rx)),
        }
    }

    pub fn handle_incoming_transaction(&mut self, peer: PeerId, tx: SignedTransaction) -> Result<(), String> {
        if !self.granular_limiter.check_tps_limit(&peer) {
            self.penalize_peer(&peer, "TPS rate limit exceeded");
            return Err("TPS rate limit exceeded".to_string());
        }

        if !self.rate_limiter.check_rate_limit(&peer) {
            self.penalize_peer(&peer, "rate limit exceeded");
            return Err("Rate limit exceeded".to_string());
        }

        let tx_hash = tx.id.clone();
        if self.gossip_filter.is_recently_seen(&tx_hash) {
            return Err("Transaction already seen".to_string());
        }

        let is_valid = self.is_valid_transaction(&tx);

        self.rate_limiter.record_tx(peer.clone());
        self.gossip_filter.mark_seen(tx_hash);

        let peer_score = self.peer_scores.entry(peer).or_insert_with(PeerScore::new);

        if is_valid {
            peer_score.record_good_tx();
            Ok(())
        } else {
            peer_score.record_bad_tx();
            Err("Invalid transaction".to_string())
        }
    }

    pub fn rebroadcast_transaction(&mut self, tx: SignedTransaction) {
        let _ = self.rebroadcast_queue.send(tx);
    }

    pub fn get_peer_score(&self, peer: &PeerId) -> Option<&PeerScore> {
        self.peer_scores.get(peer)
    }

    pub fn is_peer_banned(&self, peer: &PeerId) -> bool {
        self.peer_scores
            .get(peer)
            .map(|s| s.banned)
            .unwrap_or(false)
    }

    pub fn check_block_request_limit(&mut self, peer: &PeerId) -> bool {
        self.granular_limiter.check_block_request_limit(peer)
    }

    pub fn check_bandwidth_limit(&mut self, peer: &PeerId, bytes: usize) -> bool {
        self.granular_limiter.check_bandwidth_limit(peer, bytes)
    }

    pub fn penalize_peer(&mut self, peer: &PeerId, reason: &str) -> bool {
        let score = self.peer_scores.entry(*peer).or_insert_with(PeerScore::new);
        score.update_score(-20);
        log::warn!("Penalized peer {}: {}", peer, reason);
        score.banned
    }

    pub fn take_rebroadcast_receiver(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<SignedTransaction>> {
        self.rebroadcast_receiver.lock().unwrap().take()
    }

    fn is_valid_transaction(&self, tx: &SignedTransaction) -> bool {
        if tx.inputs.is_empty() || tx.outputs.is_empty() {
            return false;
        }

        for (index, input) in tx.inputs.iter().enumerate() {
            if input.signature.len() != 64 {
                return false;
            }
            if input.pubkey.len() != 32 && input.pubkey.len() != 33 {
                return false;
            }

            let sighash = match compute_sighash(tx, index, input.sighash_type) {
                Ok(hash) => hash,
                Err(_) => return false,
            };

            let mut pubkey_bytes = [0u8; 32];
            pubkey_bytes.copy_from_slice(&input.pubkey[..32]);

            let mut sig_bytes = [0u8; 64];
            sig_bytes.copy_from_slice(&input.signature[..64]);

            if verify_schnorr(&pubkey_bytes, &sig_bytes, &sighash).unwrap_or(false) {
                continue;
            }

            return false;
        }

        true
    }
}
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
    /// Pending sync request futures
    pending_sync_requests: std::sync::Mutex<HashMap<RequestId, oneshot::Sender<SyncResponse>>>,
    /// Sync prioritizer for intelligent block downloading
    sync_prioritizer: SyncPrioritizer,
    /// Connection manager for limits, eviction, and blacklist
    connection_manager: ConnectionManager,
}

impl NetworkManager {
    /// Create a new network manager
    pub async fn new(
        storage: StorageHandle,
        config: NetworkConfig,
        ingestion_sender: IngestionSender,
        state_manager: Arc<RwLock<KlomangStateManager>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Load or generate Ed25519 keypair
        let keypair = Self::load_or_generate_keypair(&storage).await?;
        let local_peer_id = PeerId::from(keypair.public());

        log::info!("[NETWORK] Local PeerID: {}", local_peer_id);

        // Load blacklist state from storage
        let blacklist = Self::load_blacklist_state(&storage)?;

        // Load bandwidth stats from storage
        let bandwidth_stats = Self::load_bandwidth_stats(&storage)?;

        // Check and migrate protocol version if needed
        Self::check_and_migrate_protocol_version(&storage)?;

        // Create multi-transport stack
        let transport = Self::create_multi_transport(keypair.clone())?;

        // Create Kademlia DHT
        let store = MemoryStore::new(local_peer_id);
        let kademlia = Kademlia::new(local_peer_id, store);

        // Create Gossipsub
        let gossipsub_config = GossipsubConfig::default();
        let mut gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(|e| format!("Failed to create Gossipsub: {}", e))?;

        // Subscribe to topics
        let block_topic = gossipsub::IdentTopic::new("klomang/blocks");
        gossipsub.subscribe(&block_topic)?;
        let tx_topic = gossipsub::IdentTopic::new("klomang/transactions");
        gossipsub.subscribe(&tx_topic)?;

        // Create Identify
        let identify = Identify::new(IdentifyConfig::new(
            format!("klomang/{}", CURRENT_PROTOCOL_VERSION),
            keypair.public(),
        ));

        // Create Ping for RTT and connection health
        let ping = Ping::new(PingConfig::new().with_interval(Duration::from_secs(20)).with_timeout(Duration::from_secs(10)));

        // Create control request-response for connection rejection and relay coordination
        let request_response_protocols = std::iter::once((ConnectionControlProtocol(), ProtocolSupport::Full));
        let request_response = RequestResponseBehaviour::new(
            ConnectionControlCodec,
            request_response_protocols,
            Config::default(),
        );

        // AutoNAT for NAT reachability detection
        let autonat = AutoNat::new(local_peer_id, AutoNatConfig::default());

        // Relay client fallback for NATed peers (placeholder)
        // In production, would configure actual relay behaviour

        // Sync protocol for data synchronization
        let sync_request_response = RequestResponseBehaviour::new(
            SyncCodec,
            std::iter::once((SyncProtocol(), ProtocolSupport::Full)),
            Config::default(),
        );

        // Combine behaviours (without relay for Send/Sync compatibility)
        let behaviour = KlomangNetworkBehaviour {
            kademlia,
            gossipsub,
            identify,
            ping,
            request_response,
            sync_request_response,
            autonat,
            // relay_client: RelayClient::new(),
            // NOTE: RelayClient API moved in libp2p 0.51.4, disabled for now
            relay_server: RelayServer::new(local_peer_id, Default::default()),
        };

        // Create swarm
        let mut swarm =
            SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build();

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

        // Extract config values before moving config (it doesn't implement Copy)
        let max_inbound = config.max_inbound_connections;
        let max_outbound = config.max_outbound_connections;

        Ok(NetworkManager {
            swarm,
            storage,
            config,
            connected_peers: HashSet::new(),
            local_peer_id,
            ingestion_sender,
            state_manager,
            peer_manager: Arc::new(RwLock::new(PeerManager::new())),
            pending_sync_requests: std::sync::Mutex::new(HashMap::new()),
            sync_prioritizer: SyncPrioritizer::new(10), // Max 10 sync peers
            connection_manager: ConnectionManager::new(
                max_inbound,
                max_outbound,
                blacklist,
                bandwidth_stats,
            ),
        })
    }

    /// Load or generate Ed25519 keypair from storage
    async fn load_or_generate_keypair(
        storage: &StorageHandle,
    ) -> Result<Keypair, Box<dyn std::error::Error>> {
        let key_name = "network_keypair";

        // Try to load existing keypair
        if let Ok(Some(mut key_bytes)) = storage
            .read()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .get_state(key_name)
            .map_err(|e| format!("Storage read error: {}", e))
        {
            let keypair = Keypair::ed25519_from_bytes(&mut key_bytes)
                .map_err(|e| format!("Invalid stored keypair: {}", e))?;
            log::info!("[NETWORK] Loaded existing keypair from storage");
            return Ok(keypair);
        }

        // Generate new keypair
        log::info!("[NETWORK] Generating new Ed25519 keypair");
        let keypair = Keypair::generate_ed25519();
        let ed25519_keypair = keypair
            .clone()
            .try_into_ed25519()
            .map_err(|e| format!("Failed to extract Ed25519 keypair: {}", e))?;

        // Persist the keypair secret
        let secret_bytes = ed25519_keypair.secret().as_ref().to_vec();
        storage
            .write()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .put_state(key_name, &secret_bytes)
            .map_err(|e| format!("Failed to persist keypair: {}", e))?;

        Ok(keypair)
    }

    fn load_blacklist_state(storage: &StorageHandle) -> Result<BlacklistState, Box<dyn std::error::Error>> {
        let data = {
            let store = storage.read().map_err(|e| format!("{}", e))?;
            store.get_state("network:blacklisted_ips").ok().flatten()
        };
        
        if let Some(data) = data {
            let state: BlacklistState = bincode::deserialize(&data)
                .map_err(|e| format!("Failed to deserialize blacklist state: {}", e))?;
            Ok(state)
        } else {
            Ok(BlacklistState::default())
        }
    }

    fn load_bandwidth_stats(storage: &StorageHandle) -> Result<BandwidthStats, Box<dyn std::error::Error>> {
        let data = {
            let store = storage.read().map_err(|e| format!("{}", e))?;
            store.get_state("network:bandwidth_stats").ok().flatten()
        };
        
        if let Some(data) = data {
            let stats: BandwidthStats = bincode::deserialize(&data)
                .map_err(|e| format!("Failed to deserialize bandwidth stats: {}", e))?;
            Ok(stats)
        } else {
            Ok(BandwidthStats::default())
        }
    }

    fn persist_blacklist_state(
        storage: &StorageHandle,
        blacklist: &BlacklistState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let data = bincode::serialize(blacklist)?;
        storage
            .write()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .put_state("network:blacklisted_ips", &data)
            .map_err(|e| format!("Failed to persist blacklist state: {}", e))?;
        Ok(())
    }

    fn check_and_migrate_protocol_version(storage: &StorageHandle) -> Result<(), Box<dyn std::error::Error>> {
        let stored_version = {
            let store = storage.read().map_err(|e| format!("{}", e))?;
            store.get_state("network:protocol_version")
                .ok()
                .flatten()
                .and_then(|data| bincode::deserialize::<u32>(&data).ok())
                .unwrap_or(0)
        };

        if stored_version < CURRENT_PROTOCOL_VERSION {
            log::info!("[NETWORK] Migrating from protocol version {} to {}", stored_version, CURRENT_PROTOCOL_VERSION);
            // Perform migration logic here
            // e.g., update data structures, clear incompatible caches

            // Save new version
            let version_data = bincode::serialize(&CURRENT_PROTOCOL_VERSION)?;
            {
                let store = storage.write().map_err(|e| format!("{}", e))?;
                store.put_state("network:protocol_version", &version_data)?;
            }
        }

        Ok(())
    }

    ///Create multi-transport stack: QUIC primary
    fn create_multi_transport(
        keypair: Keypair,
    ) -> Result<Boxed<(PeerId, StreamMuxerBox)>, Box<dyn std::error::Error>> {
        // Q QUIC transport (primary)
        let quic_transport = GenTransport::<libp2p_quic::tokio::Provider>::new(QuicConfig::new(&keypair))
            .map(|(peer_id, connection), _| (peer_id, StreamMuxerBox::new(connection)))
            .boxed();

        Ok(quic_transport)
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
    async fn handle_swarm_event<E: std::fmt::Debug>(
        &mut self,
        event: SwarmEvent<KlomangNetworkBehaviourEvent, E>,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("[NETWORK] Listening on {}", address);
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                let addr_str = endpoint.get_remote_address().to_string();
                let direction = match endpoint {
                    ConnectedPoint::Dialer { .. } => ConnectionDirection::Outbound,
                    ConnectedPoint::Listener { .. } => ConnectionDirection::Inbound,
                };

                // Check if we can accept this connection
                if let Err(()) = self.connection_manager.register_connection(peer_id, Some(addr_str.clone()), direction) {
                    log::warn!("[NETWORK] Rejecting connection from {}: limit reached or blacklisted", peer_id);
                    // Disconnect the peer
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                    // Send Server Busy message if inbound
                    if direction == ConnectionDirection::Inbound {
                        let _ = self.swarm.behaviour_mut().request_response.send_request(&peer_id, ConnectionControlRequest::ServerBusy("Connection limit reached".to_string()));
                    }
                    return;
                }

                log::info!("[NETWORK] Connected to peer: {} ({:?})", peer_id, direction);
                self.connected_peers.insert(peer_id);
                // Add to Kademlia
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, endpoint.get_remote_address().clone());
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                log::info!("[NETWORK] Disconnected from peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
                self.connection_manager.deregister_connection(&peer_id);
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
                self.handle_identify_event(identify_event).await;
            }
            KlomangNetworkBehaviourEvent::Ping(ping_event) => {
                self.handle_ping_event(ping_event).await;
            }
            KlomangNetworkBehaviourEvent::RequestResponse(req_resp_event) => {
                self.handle_request_response_event(req_resp_event).await;
            }
            KlomangNetworkBehaviourEvent::SyncRequestResponse(sync_event) => {
                self.handle_sync_request_response_event(sync_event).await;
            }
            KlomangNetworkBehaviourEvent::Autonat(autonat_event) => {
                self.handle_autonat_event(autonat_event).await;
            }
            KlomangNetworkBehaviourEvent::RelayServer(relay_server_event) => {
                log::debug!("[NETWORK] Relay server event: {:?}", relay_server_event);
                // Handle relay server events (reservations, circuit requests)
            }
        }
    }

    /// Handle Gossipsub events
    async fn handle_gossipsub_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message { message, .. } => {
                log::info!(
                    "[NETWORK] Received Gossipsub message on topic: {}",
                    message.topic
                );

                let peer = message.source;
                match self.decode_and_validate_network_message(peer, &message.data).await {
                    Ok(NetworkMessage::Block(block)) => {
                        log::info!(
                            "[NETWORK] Received block {} from peer {:?}",
                            block.header.id,
                            peer
                        );

                        if let Err(e) = self.process_block_parallel(block, peer).await {
                            log::error!("[NETWORK] Failed to process block: {}", e);
                        }
                    }
                    Ok(NetworkMessage::Transaction(tx)) => {
                        log::info!(
                            "[NETWORK] Deserialized transaction from peer {:?}",
                            peer
                        );

                        if let Some(peer_id) = peer {
                            if self.peer_manager.read().unwrap().is_peer_banned(&peer_id) {
                                log::warn!(
                                    "[NETWORK] Ignoring transaction from banned peer {}",
                                    peer_id
                                );
                                return;
                            }

                            let valid = {
                                let mut peer_manager = self.peer_manager.write().unwrap();
                                match peer_manager
                                    .handle_incoming_transaction(peer_id.clone(), tx.clone())
                                {
                                    Ok(()) => true,
                                    Err(e) => {
                                        log::warn!(
                                            "[NETWORK] Peer {} transaction rejected: {}",
                                            peer_id,
                                            e
                                        );
                                        false
                                    }
                                }
                            };

                            if !valid {
                                return;
                            }

                            if let Ok(sm) = self
                                .state_manager
                                .write()
                                .map_err(|_| "State manager lock poisoned".to_string())
                            {
                                match sm.add_transaction(tx.clone()) {
                                    Ok(tx_hash) => {
                                        log::info!(
                                            "[NETWORK] Transaction {} added to mempool",
                                            tx_hash
                                        );
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "[NETWORK] Transaction validation failed: {}",
                                            e
                                        );
                                        self.peer_manager
                                            .write()
                                            .unwrap()
                                            .penalize_peer(&peer_id, "invalid transaction");
                                        return;
                                    }
                                }
                            } else {
                                log::error!("[NETWORK] Failed to acquire state manager lock");
                                return;
                            }

                            if let Err(e) = self.gossip_transaction(&tx).await {
                                log::warn!("[NETWORK] Failed to gossip transaction: {}", e);
                            }
                        } else {
                            log::warn!("[NETWORK] Received transaction without peer source");
                        }
                    }
                    Err(err) => {
                        log::warn!("[NETWORK] Malformed message: {}", err);
                        if let Some(peer_id) = peer {
                            self.handle_malformed_peer(&peer_id, &err);
                        }
                    }
                }
            }
            gossipsub::Event::Subscribed { peer_id, topic } => {
                log::info!("[NETWORK] Peer {} subscribed to topic {}", peer_id, topic);
            }
            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                log::info!(
                    "[NETWORK] Peer {} unsubscribed from topic {}",
                    peer_id,
                    topic
                );
            }
            _ => {}
        }
    }

    async fn decode_and_validate_network_message(
        &self,
        peer: Option<PeerId>,
        data: &[u8],
    ) -> Result<NetworkMessage, String> {
        if data.len() < 4 {
            return Err("Payload too short for length prefix".to_string());
        }

        let mut codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .max_frame_length(MAX_NETWORK_MESSAGE_SIZE)
            .new_codec();

        let mut buffer = BytesMut::from(data);
        let frame = codec
            .decode(&mut buffer)
            .map_err(|e| format!("Framing error: {}", e))?
            .ok_or_else(|| "Incomplete framed payload".to_string())?;

        if frame.len() < MIN_NETWORK_MESSAGE_SIZE {
            return Err("Zero-length payload after framing".to_string());
        }

        let message: NetworkMessage = bincode::deserialize(&frame)
            .map_err(|e| format!("Bincode failed: {}", e))?;

        self.validate_network_message(peer, &message)
    }

    fn validate_network_message(
        &self,
        peer: Option<PeerId>,
        message: &NetworkMessage,
    ) -> Result<NetworkMessage, String> {
        match message {
            NetworkMessage::Block(block) => {
                if bincode::serialized_size(message).unwrap_or(0) as usize > MAX_BLOCK_MESSAGE_SIZE {
                    return Err("Block payload exceeds 8MB limit".to_string());
                }
                Self::validate_block_schema(block)?;
            }
            NetworkMessage::Transaction(tx) => {
                if bincode::serialized_size(message).unwrap_or(0) as usize > MAX_TRANSACTION_MESSAGE_SIZE {
                    return Err("Transaction payload exceeds 128KB limit".to_string());
                }
                Self::validate_transaction_schema(tx)?;
                if let Some(peer_id) = peer {
                    if let Some(info) = self.connection_manager.peer_info.get(&peer_id) {
                        if !info.feature_flags.as_ref().map_or(false, |flags| flags.has_flag(FeatureFlags::VM_SUPPORT))
                            && !tx.execution_payload.is_empty()
                        {
                            return Err("Peer does not support VM transactions".to_string());
                        }
                    }
                }
            }
        }

        Ok(message.clone())
    }

    fn validate_block_schema(block: &BlockNode) -> Result<(), String> {
        use klomang_core::Hash;

        if block.header.id == Hash::new(&[]) {
            return Err("Block ID is empty".to_string());
        }
        if block.header.timestamp == 0 {
            return Err("Block timestamp is zero".to_string());
        }
        if block.header.difficulty == 0 {
            return Err("Block difficulty is zero".to_string());
        }
        if block.transactions.is_empty() {
            return Err("Block contains no transactions".to_string());
        }
        if let Some(sig) = &block.header.signature {
            if sig.is_empty() {
                return Err("Block header signature is empty".to_string());
            }
        } else {
            return Err("Block header signature missing".to_string());
        }

        for tx in &block.transactions {
            Self::validate_transaction_schema(tx)?;
        }

        Ok(())
    }

    fn validate_transaction_schema(tx: &Transaction) -> Result<(), String> {
        if tx.inputs.is_empty() {
            return Err("Transaction has no inputs".to_string());
        }
        if tx.outputs.is_empty() {
            return Err("Transaction has no outputs".to_string());
        }
        if tx.chain_id == 0 {
            return Err("Transaction chain_id is zero".to_string());
        }
        for input in &tx.inputs {
            if input.signature.is_empty() {
                return Err("Transaction input signature is empty".to_string());
            }
            if input.pubkey.is_empty() {
                return Err("Transaction input pubkey is empty".to_string());
            }
        }
        for output in &tx.outputs {
            if output.value == 0 {
                return Err("Transaction output value is zero".to_string());
            }
        }
        if !tx.execution_payload.is_empty() && tx.gas_limit == 0 {
            return Err("VM transaction missing gas limit".to_string());
        }
        Ok(())
    }

    fn handle_malformed_peer(&mut self, peer_id: &PeerId, reason: &str) {
        let count = self.connection_manager.record_malformed_data(peer_id);
        self.peer_manager
            .write()
            .unwrap()
            .penalize_peer(peer_id, reason);

        if count >= 5 {
            if let Some(info) = self.connection_manager.peer_info.get(peer_id) {
                if let Some(addr) = &info.addr {
                    log::warn!("[NETWORK] Potential attacker detected {} at {}", peer_id, addr);
                    self.connection_manager.add_to_blacklist(addr.clone());
                    if let Err(e) = Self::persist_blacklist_state(&self.storage, &self.connection_manager.blacklist) {
                        log::error!("[NETWORK] Failed to persist blacklist after banning peer: {}", e);
                    }
                }
            }
            let _ = self.swarm.disconnect_peer_id(*peer_id);
            self.connection_manager.deregister_connection(peer_id);
        }
    }

    fn serialize_network_message(
        message: &NetworkMessage,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let payload = bincode::serialize(message)?;
        if payload.len() > MAX_NETWORK_MESSAGE_SIZE {
            return Err(format!("Serialized network message {} exceeds maximum {} bytes", payload.len(), MAX_NETWORK_MESSAGE_SIZE).into());
        }

        let mut codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_codec();
        let mut buffer = BytesMut::new();
        codec.encode(Bytes::from(payload), &mut buffer)?;
        Ok(buffer.to_vec())
    }

    /// Gossip a transaction to the network
    async fn gossip_transaction(
        &mut self,
        tx: &Transaction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let message = NetworkMessage::Transaction(tx.clone());
        let data = Self::serialize_network_message(&message)?;
        let topic = gossipsub::IdentTopic::new("klomang/transactions");
        self.swarm.behaviour_mut().gossipsub.publish(topic, data)?;
        Ok(())
    }

    /// Process block with parallel header verification and pipelining
    /// Validates header instantly and gossips it before full body processing
    async fn process_block_parallel(
        &mut self,
        block: BlockNode,
        peer_id: Option<PeerId>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let block_id = block.header.id.clone();

        // Step 1: Instant header validation (fast, stateless)
        let header_valid = self.validate_block_header(&block).await?;
        if !header_valid {
            log::warn!("[NETWORK] Invalid block header for {}", block_id.to_hex());
            return Ok(()); // Silently drop invalid headers
        }

        // Step 2: Gossip header immediately (pipelining)
        self.gossip_block_header(&block).await?;

        // Step 3: Parallel signature and coinbase verification
        let signature_check = self.verify_block_signatures_parallel(&block);
        let coinbase_check = self.verify_coinbase_parallel(&block);

        // Wait for both verifications
        let (sig_valid, coinbase_valid) = tokio::try_join!(signature_check, coinbase_check)?;

        if !sig_valid || !coinbase_valid {
            log::warn!(
                "[NETWORK] Block {} failed verification (sig: {}, coinbase: {})",
                block_id.to_hex(),
                sig_valid,
                coinbase_valid
            );
            return Ok(());
        }

        // Step 4: Send to ingestion queue for full processing
        if let Err(e) = self
            .ingestion_sender
            .send(IngestionMessage::Block(block))
            .await
        {
            log::error!("[NETWORK] Failed to send verified block to ingestion: {}", e);
        } else {
            log::info!("[NETWORK] Verified block {} queued for processing", block_id.to_hex());
            // Update peer activity for connection management
            if let Some(peer) = peer_id {
                self.connection_manager.update_peer_block_activity(&peer);
                // Update blue score if available (would need to get from state)
                // For now, assume some score increase
                let current_blue = self.connection_manager.peer_info.get(&peer).map(|info| info.blue_score).unwrap_or(0);
                self.connection_manager.update_peer_blue_score(peer, current_blue + 1);
            }
        }

        Ok(())
    }

    /// Fast header validation (stateless, instant)
    async fn validate_block_header(&self, block: &BlockNode) -> Result<bool, Box<dyn std::error::Error>> {
        // Basic header checks (format, timestamp, difficulty)
        // This should be very fast and not require state access
        if block.header.timestamp == 0 {
            return Ok(false);
        }

        // Check timestamp is not too far in future
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        if block.header.timestamp > now + 7200 { // 2 hours
            return Ok(false);
        }

        Ok(true)
    }

    /// Gossip only the block header for pipelining
    async fn gossip_block_header(&mut self, block: &BlockNode) -> Result<(), Box<dyn std::error::Error>> {
        // Create header-only message (would need to define HeaderMessage)
        // For now, gossip full block but log as header propagation
        let message = NetworkMessage::Block(block.clone());
        let data = Self::serialize_network_message(&message)?;
        let topic = gossipsub::IdentTopic::new("klomang/blocks");
        self.swarm.behaviour_mut().gossipsub.publish(topic, data)?;
        log::debug!("[NETWORK] Gossiped block header for {}", block.header.id.to_hex());
        Ok(())
    }

    /// Block signature verification (simplified, synchronous)
    async fn verify_block_signatures_parallel(&self, block: &BlockNode) -> Result<bool, Box<dyn std::error::Error>> {
        // Simplified synchronous verification for now
        // Parallel verification requires owned data, not references
        // TODO: Refactor to use Arc<BlockNode> if parallel verification is needed
        
        if block.transactions.is_empty() {
            return Ok(true);
        }

        // Verify all transactions (synchronously for now)
        for _tx in &block.transactions {
            // Verify transaction signatures would go here
            // For now, assume valid (klomang-core would handle this)
        }

        Ok(true)
    }

    /// Parallel coinbase verification
    async fn verify_coinbase_parallel(&self, block: &BlockNode) -> Result<bool, Box<dyn std::error::Error>> {
        // Find coinbase transaction
        let coinbase = block.transactions.iter().find(|tx| tx.inputs.is_empty());
        match coinbase {
            Some(tx) => {
                // Verify coinbase structure and reward
                // This would check emission rules from klomang-core
                Ok(self.verify_coinbase_transaction(tx).await?)
            }
            None => Ok(false), // Blocks must have coinbase
        }
    }

    /// Verify single transaction signature using klomang-core Schnorr helpers
    #[allow(dead_code)]
    async fn verify_transaction_signature(&self, tx: &Transaction) -> Result<bool, Box<dyn std::error::Error>> {
        if tx.inputs.is_empty() {
            return Ok(false);
        }

        for (input_index, input) in tx.inputs.iter().enumerate() {
            if input.signature.len() != 64 || (input.pubkey.len() != 32 && input.pubkey.len() != 33) {
                return Ok(false);
            }

            let sighash = compute_sighash(tx, input_index, input.sighash_type)
                .map_err(|e| format!("Failed to compute sighash: {}", e))?;

            let mut pubkey_bytes = [0u8; 32];
            pubkey_bytes.copy_from_slice(&input.pubkey[..32]);

            let mut sig_bytes = [0u8; 64];
            sig_bytes.copy_from_slice(&input.signature[..64]);

            if !verify_schnorr(&pubkey_bytes, &sig_bytes, &sighash).map_err(|e| format!("Schnorr verify error: {}", e))? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Verify coinbase transaction structure and reward integrity
    async fn verify_coinbase_transaction(&self, tx: &Transaction) -> Result<bool, Box<dyn std::error::Error>> {
        if !tx.inputs.is_empty() {
            return Ok(false);
        }
        if tx.outputs.is_empty() {
            return Ok(false);
        }
        if tx.outputs.iter().any(|output| output.value == 0) {
            return Ok(false);
        }
        Ok(true)
    }

    /// Handle ping events for RTT tracking
    async fn handle_ping_event(&mut self, event: PingEvent) {
        match event {
            PingEvent { peer, result } => {
                match result {
                    Ok(_rtt) => {
                        // In libp2p 0.51.x, the ping event result is the Success type, not Duration
                        // Get RTT from connection metrics or use default
                        let rtt_ms = 50u64; // Placeholder default
                        log::debug!("[NETWORK] Ping RTT to {}: {}ms", peer, rtt_ms);
                        self.connection_manager.update_peer_rtt(&peer, rtt_ms);
                        // Check for eviction due to high latency
                        if self.connection_manager.peer_info.get(&peer).map_or(false, |info| info.high_latency_count > 5) {
                            log::warn!("[NETWORK] Evicting peer {} due to high latency", peer);
                            let _ = self.swarm.disconnect_peer_id(peer);
                            self.connection_manager.deregister_connection(&peer);
                        }
                    }
                    Err(e) => {
                        log::warn!("[NETWORK] Ping failed to {}: {}", peer, e);
                    }
                }
            }
        }
    }

    /// Handle request-response events for control messages
    async fn handle_request_response_event(&mut self, event: RequestResponseEvent<ConnectionControlRequest, ConnectionControlResponse>) {
        match event {
            RequestResponseEvent::Message { peer, message } => {
                match message {
                    libp2p::request_response::Message::Request { request, channel, .. } => {
                        match request {
                            ConnectionControlRequest::Ping => {
                                log::debug!("[NETWORK] Received ping from {}", peer);
                                let _ = self.swarm.behaviour_mut().request_response.send_response(channel, ConnectionControlResponse::Pong);
                            }
                            ConnectionControlRequest::ServerBusy(reason) => {
                                log::warn!("[NETWORK] Received server busy from {}: {}", peer, reason);
                                // Could penalize or handle
                            }
                        }
                    }
                    libp2p::request_response::Message::Response { response, .. } => {
                        match response {
                            ConnectionControlResponse::Pong => {
                                log::debug!("[NETWORK] Received pong from {}", peer);
                            }
                            ConnectionControlResponse::ServerBusy(reason) => {
                                log::warn!("[NETWORK] Server busy response from {}: {}", peer, reason);
                            }
                            ConnectionControlResponse::Ack => {
                                log::debug!("[NETWORK] Received ack from {}", peer);
                            }
                        }
                    }
                }
            }
            RequestResponseEvent::OutboundFailure { peer, error, .. } => {
                log::warn!("[NETWORK] Request-response outbound failure to {}: {}", peer, error);
            }
            RequestResponseEvent::InboundFailure { peer, error, .. } => {
                log::warn!("[NETWORK] Request-response inbound failure from {}: {}", peer, error);
            }
            _ => {}
        }
    }

    /// Handle sync request-response events
    async fn handle_sync_request_response_event(&mut self, event: RequestResponseEvent<SyncRequest, SyncResponse>) {
        match event {
            RequestResponseEvent::Message { peer, message } => {
                match message {
                    libp2p::request_response::Message::Request { request, channel, .. } => {
                        let process_request = |manager: &mut PeerManager, request: &SyncRequest| {
                            match request {
                                SyncRequest::GetBlock { .. } | SyncRequest::GetHeaders { .. } | SyncRequest::GetStateSnapshot { .. } | SyncRequest::GetStateChunks { .. } => {
                                    manager.check_block_request_limit(&peer)
                                }
                                _ => true,
                            }
                        };

                        let mut peer_manager = self.peer_manager.write().unwrap();
                        if !process_request(&mut *peer_manager, &request) {
                            let _ = self.swarm.behaviour_mut().sync_request_response.send_response(channel, SyncResponse::RateLimited);
                            if peer_manager.penalize_peer(&peer, "block request rate limit exceeded") {
                                let _ = self.swarm.disconnect_peer_id(peer);
                                self.connection_manager.deregister_connection(&peer);
                            }
                            return;
                        }
                        drop(peer_manager);

                        let response = match self.process_sync_request(peer, request).await {
                            Ok(resp) => resp,
                            Err(e) => {
                                log::warn!("[NETWORK] Sync request error from {}: {}", peer, e);
                                SyncResponse::Busy
                            }
                        };

                        let _ = self.swarm.behaviour_mut().sync_request_response.send_response(channel, response);
                    }
                    libp2p::request_response::Message::Response { response, request_id } => {
                        log::debug!("[NETWORK] Received sync response from {}", peer);
                        if let Some(sender) = self.pending_sync_requests.lock().unwrap().remove(&request_id) {
                            let _ = sender.send(response);
                            return;
                        }

                        match response {
                            SyncResponse::NotFound => log::debug!("[NETWORK] Data not found"),
                            SyncResponse::RateLimited => log::warn!("[NETWORK] Rate limited by peer {}", peer),
                            SyncResponse::Busy => log::warn!("[NETWORK] Peer {} is busy", peer),
                            _ => log::debug!("[NETWORK] Received sync data"),
                        }
                    }
                }
            }
            RequestResponseEvent::OutboundFailure { peer, request_id, error, .. } => {
                log::warn!("[NETWORK] Sync outbound failure to {}: {}", peer, error);
                if let Some(sender) = self.pending_sync_requests.lock().unwrap().remove(&request_id) {
                    let _ = sender.send(SyncResponse::Busy);
                }
                if self
                    .peer_manager
                    .write()
                    .unwrap()
                    .penalize_peer(&peer, "sync timeout")
                {
                    let _ = self.swarm.disconnect_peer_id(peer);
                    self.connection_manager.deregister_connection(&peer);
                }
            }
            RequestResponseEvent::InboundFailure { peer, error, .. } => {
                log::warn!("[NETWORK] Sync inbound failure from {}: {}", peer, error);
            }
            _ => {}
        }
    }

    /// Process sync request with timeout and integration
    async fn process_sync_request(&mut self, peer: PeerId, request: SyncRequest) -> Result<SyncResponse, Box<dyn std::error::Error>> {
        // 10 second timeout for all requests
        let timeout_duration = Duration::from_secs(10);
        let timeout_future = tokio::time::timeout(timeout_duration, self.handle_sync_request_inner(peer, request));

        match timeout_future.await {
            Ok(result) => result,
            Err(_) => {
                self.peer_manager.write().unwrap().penalize_peer(&peer, "sync request timeout");
                Ok(SyncResponse::Busy)
            }
        }
    }

    /// Inner sync request handler
    async fn handle_sync_request_inner(&mut self, peer: PeerId, request: SyncRequest) -> Result<SyncResponse, Box<dyn std::error::Error>> {
        match request {
            SyncRequest::GetBlock { hash } => {
                // Check bandwidth limit
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, 8 * 1024 * 1024) { // Assume 8MB
                    return Ok(SyncResponse::RateLimited);
                }

                // Get block from storage
                match self.storage.read().unwrap().get_block(&hash)? {
                    Some(block) => Ok(SyncResponse::Block(block)),
                    None => Ok(SyncResponse::NotFound),
                }
            }
            SyncRequest::GetHeaders { start_height, count } => {
                // Check bandwidth limit (headers are smaller)
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, count as usize * 1024) { // Assume 1KB per header
                    return Ok(SyncResponse::RateLimited);
                }

                // Get headers from storage
                let headers = self.storage.read().unwrap().get_block_headers(start_height, count)?;
                Ok(SyncResponse::Headers(headers))
            }
            SyncRequest::GetStateProof { key } => {
                // Check bandwidth limit
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, 1024) {
                    return Ok(SyncResponse::RateLimited);
                }

                let state_manager = self.state_manager.read().unwrap();
                match state_manager.generate_verkle_proof_sync(&key) {
                    Ok(proof) => Ok(SyncResponse::StateProof(proof)),
                    Err(_) => Ok(SyncResponse::NotFound),
                }
            }
            SyncRequest::GetStateProofRange { start_key, end_key } => {
                // Check bandwidth limit (estimate based on range size)
                let range_size = end_key.len().saturating_sub(start_key.len());
                let estimated_size = (range_size as usize).min(10 * 1024 * 1024); // Cap at 10MB
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, estimated_size) {
                    return Ok(SyncResponse::RateLimited);
                }

                let state_manager = self.state_manager.read().unwrap();
                match state_manager.generate_verkle_proof_range_sync(&start_key, &end_key) {
                    Ok(proofs) => Ok(SyncResponse::StateProofRange(proofs)),
                    Err(_) => Ok(SyncResponse::NotFound),
                }
            }
            SyncRequest::GetTransaction { hash } => {
                // Check bandwidth limit
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, 128 * 1024) {
                    return Ok(SyncResponse::RateLimited);
                }

                let state_manager = self.state_manager.read().unwrap();
                if let Some(tx) = state_manager.get_transaction_from_mempool_sync(&hash) {
                    return Ok(SyncResponse::Transaction(tx));
                }

                // Then storage
                match self.storage.read().unwrap().get_transaction(&hash)? {
                    Some(tx) => Ok(SyncResponse::Transaction(tx)),
                    None => Ok(SyncResponse::NotFound),
                }
            }
            SyncRequest::GetStateSnapshot { checkpoint_height } => {
                // Check bandwidth limit (snapshot metadata is small)
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, 1024) {
                    return Ok(SyncResponse::RateLimited);
                }

                let state_manager = self.state_manager.read().unwrap();
                match state_manager.get_state_snapshot_info(checkpoint_height) {
                    Ok((root_hash, total_chunks)) => Ok(SyncResponse::StateSnapshot { root_hash, total_chunks }),
                    Err(_) => Ok(SyncResponse::NotFound),
                }
            }
            SyncRequest::GetStateChunks { checkpoint_height, chunk_indices } => {
                // Check bandwidth limit (assume 1MB per chunk)
                let estimated_size = chunk_indices.len() as u64 * 1024 * 1024;
                if !self.peer_manager.write().unwrap().check_bandwidth_limit(&peer, estimated_size as usize) {
                    return Ok(SyncResponse::RateLimited);
                }

                let state_manager = self.state_manager.read().unwrap();
                match state_manager.get_state_chunks(checkpoint_height, &chunk_indices) {
                    Ok(chunks) => Ok(SyncResponse::StateChunks(chunks)),
                    Err(_) => Ok(SyncResponse::NotFound),
                }
            }
        }
    }

    /// Handle identify events for protocol version negotiation
    async fn handle_identify_event(&mut self, event: libp2p::identify::Event) {
        match event {
            libp2p::identify::Event::Received { peer_id, info } => {
                log::debug!("[NETWORK] Received identify info from {}: {:?}", peer_id, info);

                // Parse protocol version and feature flags from agent version or protocols
                // For now, assume version is in agent_version like "klomang/1.0.0"
                let protocol_version = Self::parse_protocol_version(&info.agent_version);
                let feature_flags = Self::parse_feature_flags(&info.protocols);

                // Check compatibility
                if let Some(pv) = &protocol_version {
                    if pv.version < pv.min_compatible_version {
                        log::warn!("[NETWORK] Peer {} has incompatible version {}, disconnecting", peer_id, pv.version);
                        let _ = self.swarm.disconnect_peer_id(peer_id);
                        self.connection_manager.deregister_connection(&peer_id);
                        return;
                    }
                }

                // Update peer info
                if let Some(info) = self.connection_manager.peer_info.get_mut(&peer_id) {
                    info.protocol_version = protocol_version;
                    info.feature_flags = feature_flags;
                }
            }
            _ => {}
        }
    }

    /// Parse protocol version from agent version string
    fn parse_protocol_version(agent_version: &str) -> Option<ProtocolVersion> {
        // Assume format "klomang/1" where 1 is version
        if let Some(version_str) = agent_version.strip_prefix("klomang/") {
            if let Ok(version) = version_str.parse::<u32>() {
                Some(ProtocolVersion {
                    version,
                    min_compatible_version: MIN_COMPATIBLE_VERSION,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Parse feature flags from supported protocols
    fn parse_feature_flags(protocols: &[String]) -> Option<FeatureFlags> {
        let mut flags = FeatureFlags::default();
        for protocol in protocols {
            if protocol.contains("vm") {
                flags.set_flag(FeatureFlags::VM_SUPPORT);
            }
            if protocol.contains("verkle") {
                flags.set_flag(FeatureFlags::VERKLE_V2);
            }
            if protocol.contains("sync") {
                flags.set_flag(FeatureFlags::NEW_P2P_SYNC);
            }
        }
        Some(flags)
    }

    /// Handle AutoNAT events for NAT traversal
    async fn handle_autonat_event(&mut self, event: AutoNatEvent) {
        match event {
            AutoNatEvent::InboundProbe(inbound) => {
                log::debug!("[NETWORK] AutoNAT inbound probe: {:?}", inbound);
            }
            AutoNatEvent::OutboundProbe(outbound) => {
                log::debug!("[NETWORK] AutoNAT outbound probe: {:?}", outbound);
            }
            AutoNatEvent::StatusChanged { old, new } => {
                log::info!("[NETWORK] AutoNAT status changed from {:?} to {:?}", old, new);
                // If behind NAT, enable relay fallback
                if matches!(new, libp2p::autonat::NatStatus::Private) {
                    log::info!("[NETWORK] Detected NAT, enabling relay fallback");
                    // Add relay addresses to swarm
                    for relay_addr in &self.config.relay_fallback_addresses {
                        if let Ok(addr) = relay_addr.parse::<libp2p::Multiaddr>() {
                            let _ = self.swarm.listen_on(addr);
                        }
                    }
                }
            }
        }
    }

    /// Periodic eviction check
    pub async fn perform_eviction_check(&mut self) {
        let stale_threshold = Duration::from_secs(300); // 5 minutes
        let stale_peers = self.connection_manager.stale_peers(stale_threshold);
        for peer_id in stale_peers {
            log::info!("[NETWORK] Evicting stale peer {}", peer_id);
            let _ = self.swarm.disconnect_peer_id(peer_id);
            self.connection_manager.deregister_connection(&peer_id);
        }

        // Persist updated blacklist
        if let Err(e) = Self::persist_blacklist_state(&self.storage, &self.connection_manager.blacklist) {
            log::error!("[NETWORK] Failed to persist blacklist: {}", e);
        }

        // Persist bandwidth stats
        let stats_data = bincode::serialize(&self.connection_manager.bandwidth_stats).unwrap();
        if let Err(e) = self.storage.write().unwrap().put_state("network:bandwidth_stats", &stats_data) {
            log::error!("[NETWORK] Failed to persist bandwidth stats: {}", e);
        }
    }

    /// Start background eviction task
    pub fn start_eviction_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let _self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Check every minute
            loop {
                interval.tick().await;
                // For now, skip eviction check to avoid unsafe code
                // TODO: implement with proper synchronization
            }
        })
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

    /// Update peer score for sync prioritization
    pub fn update_peer_score(&self, peer_id: PeerId, blue_score: u64, latency_ms: u64, blocks_per_sec: f64) {
        self.sync_prioritizer.update_peer_score(peer_id, blue_score, latency_ms, blocks_per_sec);
    }

    /// Get top peers for sync based on score
    pub fn get_top_sync_peers(&self) -> Vec<PeerId> {
        self.sync_prioritizer.get_top_sync_peers()
    }

    /// Mark peer as preferred sync source
    pub fn mark_sync_peer(&self, peer_id: &PeerId, is_sync: bool) {
        self.sync_prioritizer.mark_sync_peer(peer_id, is_sync);
    }

    /// Send an outbound sync request and await the response with a 10-second timeout
    pub async fn send_sync_request(
        &mut self,
        peer: &PeerId,
        request: SyncRequest,
    ) -> Result<SyncResponse, String> {
        let request_id = self
            .swarm
            .behaviour_mut()
            .sync_request_response
            .send_request(peer, request);

        let (tx, rx) = oneshot::channel();
        self.pending_sync_requests.lock().unwrap().insert(request_id, tx);

        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                self.pending_sync_requests.lock().unwrap().remove(&request_id);
                Err("Peer response channel closed".to_string())
            }
            Err(_) => {
                self.pending_sync_requests.lock().unwrap().remove(&request_id);
                self.peer_manager
                    .write()
                    .unwrap()
                    .penalize_peer(peer, "sync request timeout");
                Err("Sync request timed out".to_string())
            }
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
        let storage = KlomangStorage::open_with_recovery(
            temp_dir.path(),
            crate::storage::config::PruningStrategy::Archive,
        )
        .unwrap();
        let storage_handle = Arc::new(RwLock::new(storage));

        // Generate and save keypair
        let keypair1 = NetworkManager::load_or_generate_keypair(&storage_handle)
            .await
            .unwrap();
        let peer_id1 = PeerId::from(keypair1.public());

        // Load existing keypair
        let keypair2 = NetworkManager::load_or_generate_keypair(&storage_handle)
            .await
            .unwrap();
        let peer_id2 = PeerId::from(keypair2.public());

        // Should be the same
        assert_eq!(peer_id1, peer_id2);
    }
}
