use crate::network::config::{FeatureFlags, ProtocolVersion};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Connection direction for peer sockets
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDirection {
    Inbound,
    Outbound,
}

/// Per-peer connection metadata for eviction and load balancing
#[derive(Debug, Clone)]
pub struct PeerConnectionInfo {
    pub peer_id: PeerId,
    pub addr: Option<String>,
    pub direction: ConnectionDirection,
    pub bad_data_count: u32,
    pub high_latency_count: u32,
    pub last_rtt_ms: u64,
    pub last_block_seen: Option<Instant>,
    pub last_message_seen: Instant,
    pub blue_score: u64,
    pub protocol_version: Option<ProtocolVersion>,
    pub feature_flags: Option<FeatureFlags>,
}

impl PeerConnectionInfo {
    pub fn new(peer_id: PeerId, addr: Option<String>, direction: ConnectionDirection) -> Self {
        Self {
            peer_id,
            addr,
            direction,
            bad_data_count: 0,
            high_latency_count: 0,
            last_rtt_ms: 0,
            last_block_seen: None,
            last_message_seen: Instant::now(),
            blue_score: 0,
            protocol_version: None,
            feature_flags: None,
        }
    }

    pub fn priority_score(&self) -> f64 {
        let blue = self.blue_score as f64;
        let latency = if self.last_rtt_ms == 0 {
            1000.0
        } else {
            1.0 / (self.last_rtt_ms as f64 + 1.0)
        };
        let recency = 1.0 / (self.last_message_seen.elapsed().as_secs_f64() + 1.0);
        blue * 0.6 + latency * 0.3 + recency * 0.1
    }
}

/// Persistent blacklist storage for banned IPs
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlacklistState {
    pub blacklisted_ips: HashSet<String>,
}

impl Default for BlacklistState {
    fn default() -> Self {
        Self {
            blacklisted_ips: HashSet::new(),
        }
    }
}

/// Bandwidth usage statistics per transport
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BandwidthStats {
    pub quic_bytes_sent: u64,
    pub quic_bytes_received: u64,
    pub tcp_bytes_sent: u64,
    pub tcp_bytes_received: u64,
    pub ws_bytes_sent: u64,
    pub ws_bytes_received: u64,
    pub last_updated: u64, // timestamp
}

impl Default for BandwidthStats {
    fn default() -> Self {
        Self {
            quic_bytes_sent: 0,
            quic_bytes_received: 0,
            tcp_bytes_sent: 0,
            tcp_bytes_received: 0,
            ws_bytes_sent: 0,
            ws_bytes_received: 0,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Connection manager with inbound/outbound limits and eviction policy
pub struct ConnectionManager {
    pub max_inbound: usize,
    pub max_outbound: usize,
    pub inbound_count: usize,
    pub outbound_count: usize,
    pub peer_info: HashMap<PeerId, PeerConnectionInfo>,
    pub blacklist: BlacklistState,
    pub bandwidth_stats: BandwidthStats,
}

impl ConnectionManager {
    pub fn new(max_inbound: usize, max_outbound: usize, blacklist: BlacklistState, bandwidth_stats: BandwidthStats) -> Self {
        Self {
            max_inbound,
            max_outbound,
            inbound_count: 0,
            outbound_count: 0,
            peer_info: HashMap::new(),
            blacklist,
            bandwidth_stats,
        }
    }

    pub fn is_blacklisted_ip(&self, ip: &str) -> bool {
        self.blacklist.blacklisted_ips.contains(ip)
    }

    pub fn add_to_blacklist(&mut self, ip: String) {
        self.blacklist.blacklisted_ips.insert(ip);
    }

    pub fn should_accept_connection(&self, direction: ConnectionDirection) -> bool {
        match direction {
            ConnectionDirection::Inbound => self.inbound_count < self.max_inbound,
            ConnectionDirection::Outbound => self.outbound_count < self.max_outbound,
        }
    }

    pub fn register_connection(
        &mut self,
        peer_id: PeerId,
        addr: Option<String>,
        direction: ConnectionDirection,
    ) -> Result<(), ()> {
        if self.peer_info.contains_key(&peer_id) {
            return Ok(());
        }

        if !self.should_accept_connection(direction) {
            return Err(());
        }

        if let Some(addr_str) = &addr {
            if self.is_blacklisted_ip(addr_str) {
                return Err(());
            }
        }

        let info = PeerConnectionInfo::new(peer_id.clone(), addr.clone(), direction);
        match direction {
            ConnectionDirection::Inbound => self.inbound_count += 1,
            ConnectionDirection::Outbound => self.outbound_count += 1,
        }
        self.peer_info.insert(peer_id, info);
        Ok(())
    }

    pub fn deregister_connection(&mut self, peer_id: &PeerId) {
        if let Some(info) = self.peer_info.remove(peer_id) {
            match info.direction {
                ConnectionDirection::Inbound => self.inbound_count = self.inbound_count.saturating_sub(1),
                ConnectionDirection::Outbound => self.outbound_count = self.outbound_count.saturating_sub(1),
            }
        }
    }

    pub fn update_peer_rtt(&mut self, peer_id: &PeerId, rtt_ms: u64) {
        if let Some(info) = self.peer_info.get_mut(peer_id) {
            info.last_rtt_ms = rtt_ms;
            info.last_message_seen = Instant::now();
            if rtt_ms > 2000 {
                info.high_latency_count += 1;
            } else {
                info.high_latency_count = 0;
            }
        }
    }

    pub fn update_peer_block_activity(&mut self, peer_id: &PeerId) {
        if let Some(info) = self.peer_info.get_mut(peer_id) {
            info.last_block_seen = Some(Instant::now());
            info.last_message_seen = Instant::now();
        }
    }

    pub fn update_peer_blue_score(&mut self, peer_id: PeerId, blue_score: u64) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info.blue_score = blue_score;
            info.last_message_seen = Instant::now();
        }
    }

    pub fn record_bad_data(&mut self, peer_id: &PeerId) -> bool {
        if let Some(info) = self.peer_info.get_mut(peer_id) {
            info.bad_data_count += 1;
            info.last_message_seen = Instant::now();
            return info.bad_data_count > 3;
        }
        false
    }

    pub fn record_malformed_data(&mut self, peer_id: &PeerId) -> u32 {
        if let Some(info) = self.peer_info.get_mut(peer_id) {
            info.bad_data_count += 1;
            info.last_message_seen = Instant::now();
            return info.bad_data_count;
        }
        0
    }

    pub fn stale_peers(&self, threshold: Duration) -> Vec<PeerId> {
        self.peer_info
            .iter()
            .filter_map(|(peer_id, info)| {
                if let Some(last_block) = info.last_block_seen {
                    if last_block.elapsed() > threshold && info.blue_score < 100 {
                        return Some(peer_id.clone());
                    }
                } else if info.last_message_seen.elapsed() > threshold {
                    return Some(peer_id.clone());
                }
                None
            })
            .collect()
    }

    pub fn update_bandwidth(&mut self, transport: &str, bytes_sent: u64, bytes_received: u64) {
        match transport {
            "quic" => {
                self.bandwidth_stats.quic_bytes_sent += bytes_sent;
                self.bandwidth_stats.quic_bytes_received += bytes_received;
            }
            "tcp" => {
                self.bandwidth_stats.tcp_bytes_sent += bytes_sent;
                self.bandwidth_stats.tcp_bytes_received += bytes_received;
            }
            "ws" => {
                self.bandwidth_stats.ws_bytes_sent += bytes_sent;
                self.bandwidth_stats.ws_bytes_received += bytes_received;
            }
            _ => {}
        }
        self.bandwidth_stats.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}
