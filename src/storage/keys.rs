/// Prefix & Key Design untuk Blockchain Storage
/// Menggunakan prefix-based key scheme untuk memudahkan iterasi dan prefix scanning
/// Standar consensus dengan klomang-core
use klomang_core::core::crypto::Hash;

/// Prefix untuk block storage: b<hash_in_hex>
pub const BLOCK_PREFIX: &[u8] = b"b";

/// Prefix untuk transaction storage: t<hash_in_hex>
#[allow(dead_code)]
pub const TX_PREFIX: &[u8] = b"t";

/// Prefix untuk height-based index: i<height_as_u64_be>
pub const HEIGHT_INDEX_PREFIX: &[u8] = b"i";

/// Prefix untuk metadata storage: m<key_name>
/// Digunakan untuk menyimpan internal state seperti schema version, sync status, etc
pub const META_PREFIX: &[u8] = b"m";

/// Metadata key constants
pub const CURRENT_DB_VERSION: u32 = 1; // Current schema version

/// Helper struct untuk memanage key generation secara konsisten
pub struct KeyBuilder;

impl KeyBuilder {
    /// Generate block key dari hash
    /// Format: b<hex_string_of_hash>
    pub fn block_key(hash: &Hash) -> Vec<u8> {
        let mut key = Vec::with_capacity(BLOCK_PREFIX.len() + 64);
        key.extend_from_slice(BLOCK_PREFIX);
        key.extend_from_slice(hash.to_hex().as_bytes());
        key
    }

    /// Generate transaction key dari hash
    /// Format: t<hex_string_of_hash>
    #[allow(dead_code)]
    pub fn tx_key(hash: &Hash) -> Vec<u8> {
        let mut key = Vec::with_capacity(TX_PREFIX.len() + 64);
        key.extend_from_slice(TX_PREFIX);
        key.extend_from_slice(hash.to_hex().as_bytes());
        key
    }

    /// Generate height index key dari block height
    /// Format: i<height_as_u64_big_endian>
    /// Memungkinkan efficient range query untuk height scanning
    pub fn height_index_key(height: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(HEIGHT_INDEX_PREFIX.len() + 8);
        key.extend_from_slice(HEIGHT_INDEX_PREFIX);
        key.extend_from_slice(&height.to_be_bytes());
        key
    }

    /// Extract height dari height index key
    /// Returns Ok(height) jika key valid, Err(msg) jika format invalid
    #[allow(dead_code)]
    pub fn extract_height(key: &[u8]) -> Result<u64, String> {
        if key.len() < HEIGHT_INDEX_PREFIX.len() + 8 {
            return Err("Height index key terlalu pendek".to_string());
        }

        if !key.starts_with(HEIGHT_INDEX_PREFIX) {
            return Err("Height index key prefix tidak valid".to_string());
        }

        let height_bytes: [u8; 8] = key[HEIGHT_INDEX_PREFIX.len()..HEIGHT_INDEX_PREFIX.len() + 8]
            .try_into()
            .map_err(|_| "Gagal mengonversi height bytes".to_string())?;

        Ok(u64::from_be_bytes(height_bytes))
    }

    /// Generate metadata key untuk internal state
    /// Format: m<metadata_key_name>
    /// Digunakan untuk schema version, sync metadata, etc
    pub fn metadata_key(key: &str) -> Vec<u8> {
        let mut full_key = Vec::with_capacity(META_PREFIX.len() + key.len());
        full_key.extend_from_slice(META_PREFIX);
        full_key.extend_from_slice(key.as_bytes());
        full_key
    }

    /// Generate database version metadata key
    pub fn db_version_key() -> Vec<u8> {
        Self::metadata_key("db_version")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_key_generation() {
        let hash = Hash::new(b"test_block");
        let key = KeyBuilder::block_key(&hash);
        assert!(key.starts_with(BLOCK_PREFIX));
        assert!(key.len() > BLOCK_PREFIX.len());
    }

    #[test]
    fn test_tx_key_generation() {
        let hash = Hash::new(b"test_tx");
        let key = KeyBuilder::tx_key(&hash);
        assert!(key.starts_with(TX_PREFIX));
        assert!(key.len() > TX_PREFIX.len());
    }

    #[test]
    fn test_height_index_key_generation() {
        let height = 12345u64;
        let key = KeyBuilder::height_index_key(height);
        assert!(key.starts_with(HEIGHT_INDEX_PREFIX));
        assert_eq!(key.len(), HEIGHT_INDEX_PREFIX.len() + 8);

        let extracted = KeyBuilder::extract_height(&key).expect("Gagal extract height");
        assert_eq!(extracted, height);
    }

    #[test]
    fn test_height_extraction_invalid() {
        let invalid_key = b"x1234";
        assert!(KeyBuilder::extract_height(invalid_key).is_err());
    }
}
