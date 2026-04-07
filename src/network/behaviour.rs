use crate::network::protocol::{ConnectionControlCodec, SyncCodec};
use libp2p::{autonat::Behaviour as AutoNat, gossipsub::Behaviour as Gossipsub, identify::Behaviour as Identify, kad::{Kademlia, store::MemoryStore}, ping::Behaviour as Ping, relay::Behaviour as RelayServer, request_response::Behaviour as RequestResponseBehaviour, swarm::NetworkBehaviour};

#[derive(NetworkBehaviour)]
pub struct KlomangNetworkBehaviour {
    /// Kademlia DHT for peer discovery
    pub kademlia: Kademlia<MemoryStore>,
    /// Gossipsub for efficient block and transaction propagation
    pub gossipsub: Gossipsub,
    /// Identify for exchanging node information
    pub identify: Identify,
    /// Ping for RTT tracking and NAT detection
    pub ping: Ping,
    /// Control stream for connection management and server-busy signaling
    pub request_response: RequestResponseBehaviour<ConnectionControlCodec>,
    /// Sync protocol for data synchronization
    pub sync_request_response: RequestResponseBehaviour<SyncCodec>,
    /// AutoNAT for NAT reachability detection
    pub autonat: AutoNat,
    /// Relay server for acting as hop for other nodes
    pub relay_server: RelayServer,
}
