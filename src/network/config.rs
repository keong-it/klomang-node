use serde::{Deserialize, Serialize};

/// Network configuration
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetworkConfig {
    /// Listen addresses for incoming connections
    pub listen_addresses: Vec<String>,
    /// Bootstrap peers to connect to on startup
    pub bootstrap_peers: Vec<String>,
    /// Maximum number of inbound connections
    pub max_inbound_connections: usize,
    /// Maximum number of outbound connections
    pub max_outbound_connections: usize,
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    /// Relay fallback addresses to use when direct NAT connectivity fails
    pub relay_fallback_addresses: Vec<String>,
    /// Enable relay server (hop) functionality
    pub enable_relay_server: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            listen_addresses: vec![
                "/ip4/0.0.0.0/udp/3833/quic-v1".to_string(),
                "/ip6/::/udp/3833/quic-v1".to_string(),
                "/ip4/0.0.0.0/tcp/3834".to_string(),
                "/ip6/::/tcp/3834".to_string(),
                "/ip4/0.0.0.0/tcp/3835/ws".to_string(),
                "/ip6/::/tcp/3835/ws".to_string(),
            ],
            bootstrap_peers: vec![
                // Replace with real seed node multiaddrs when available
            ],
            max_inbound_connections: 128,
            max_outbound_connections: 16,
            connection_timeout: 30,
            relay_fallback_addresses: Vec::new(),
            enable_relay_server: true,
        }
    }
}

/// Protocol version information
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtocolVersion {
    pub version: u32,
    pub min_compatible_version: u32,
}

/// Feature flags bitmask for peer capabilities
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FeatureFlags(pub u32);

impl FeatureFlags {
    pub const VM_SUPPORT: u32 = 0x01;
    pub const VERKLE_V2: u32 = 0x02;
    pub const NEW_P2P_SYNC: u32 = 0x04;

    pub fn has_flag(&self, flag: u32) -> bool {
        (self.0 & flag) != 0
    }

    pub fn set_flag(&mut self, flag: u32) {
        self.0 |= flag;
    }
}

impl Default for FeatureFlags {
    fn default() -> Self {
        FeatureFlags(0)
    }
}
