use async_trait::async_trait;
use bincode;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::{Codec, ProtocolName};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use klomang_core::{BlockNode, Transaction};

/// Network message types for gossip protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    Block(BlockNode),
    Transaction(Transaction),
}

/// Control messages for dedicated connection management
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConnectionControlRequest {
    ServerBusy(String),
    Ping,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConnectionControlResponse {
    Ack,
    ServerBusy(String),
    Pong,
}

#[derive(Clone)]
pub struct ConnectionControlProtocol();

impl ProtocolName for ConnectionControlProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/klomang/connection-control/1.0.0"
    }
}

#[derive(Clone)]
pub struct ConnectionControlCodec;

#[async_trait]
impl Codec for ConnectionControlCodec {
    type Protocol = ConnectionControlProtocol;
    type Request = ConnectionControlRequest;
    type Response = ConnectionControlResponse;

    async fn read_request<T>(&mut self, _protocol: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(&mut self, _protocol: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(&mut self, _protocol: &Self::Protocol, io: &mut T, req: Self::Request) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = bincode::serialize(&req).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        io.write_all(&data).await
    }

    async fn write_response<T>(&mut self, _protocol: &Self::Protocol, io: &mut T, res: Self::Response) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = bincode::serialize(&res).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        io.write_all(&data).await
    }
}

#[derive(Clone)]
pub struct SyncProtocol();

impl ProtocolName for SyncProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/klomang/sync/1.0.0"
    }
}

#[derive(Clone)]
pub struct SyncCodec;

#[async_trait]
impl Codec for SyncCodec {
    type Protocol = SyncProtocol;
    type Request = SyncRequest;
    type Response = SyncResponse;

    async fn read_request<T>(&mut self, _protocol: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(&mut self, _protocol: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(&mut self, _protocol: &Self::Protocol, io: &mut T, req: Self::Request) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = bincode::serialize(&req).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        io.write_all(&data).await
    }

    async fn write_response<T>(&mut self, _protocol: &Self::Protocol, io: &mut T, res: Self::Response) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = bincode::serialize(&res).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        io.write_all(&data).await
    }
}

/// State chunk for fast sync
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateChunk {
    pub index: u32,
    pub data: Vec<u8>,
    pub checksum: klomang_core::Hash,
}

/// Request types for sync protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncRequest {
    GetBlock { hash: klomang_core::Hash },
    GetHeaders { start_height: u64, count: u32 },
    GetStateProof { key: Vec<u8> },
    GetStateProofRange { start_key: Vec<u8>, end_key: Vec<u8> },
    GetTransaction { hash: klomang_core::Hash },
    GetStateSnapshot { checkpoint_height: u64 },
    GetStateChunks { checkpoint_height: u64, chunk_indices: Vec<u32> },
}

/// Response types for sync protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncResponse {
    Block(BlockNode),
    Headers(Vec<klomang_core::BlockHeader>),
    StateProof(Vec<u8>),
    StateProofRange(Vec<(Vec<u8>, Vec<u8>)>), // Vec<(key, proof)>
    Transaction(Transaction),
    NotFound,
    RateLimited,
    Busy,
    StateSnapshot { root_hash: klomang_core::Hash, total_chunks: u32 },
    StateChunks(Vec<StateChunk>),
}

/// Encode a network message into a length-delimited frame.
pub fn encode_network_message<T: Serialize>(message: &T) -> Result<Vec<u8>, String> {
    let mut codec = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .max_frame_length(8 * 1024 * 1024)
        .new_codec();
    let mut buffer = BytesMut::new();
    let payload = bincode::serialize(message).map_err(|e| e.to_string())?;
    codec.encode(Bytes::from(payload), &mut buffer).map_err(|e| e.to_string())?;
    Ok(buffer.to_vec())
}

/// Decode a length-delimited network message frame.
pub fn decode_network_message<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T, String> {
    let mut codec = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .max_frame_length(8 * 1024 * 1024)
        .new_codec();
    let mut buffer = BytesMut::from(data);
    let frame = codec
        .decode(&mut buffer)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Incomplete framed payload".to_string())?;
    bincode::deserialize(&frame).map_err(|e| e.to_string())
}
