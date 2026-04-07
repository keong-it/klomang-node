//! RPC Interface for Klomang Node
//!
//! Provides HTTP JSON-RPC endpoints for:
//! - Transaction submission
//! - Block queries
//! - Contract calls (eth_call equivalent)
//! - State queries

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;
use warp::Filter;

use crate::state::KlomangStateManager;
use hex;
use klomang_core::SignedTransaction;

/// RPC request for contract call
#[derive(Deserialize)]
struct EthCallRequest {
    to: Option<String>, // Contract address
    data: String,       // Call data (hex)
    gas_limit: Option<u64>,
}

#[derive(Deserialize)]
struct EstimateGasRequest {
    to: Option<String>,
    data: String,
    gas_limit: Option<u64>,
}

/// RPC response
#[derive(Serialize)]
struct RpcResponse<T> {
    jsonrpc: String,
    id: Option<u64>,
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

/// Contract call result
#[derive(Serialize)]
struct CallResult {
    data: String, // Return data (hex)
    gas_used: u64,
}

/// RPC Server
pub struct RpcServer {
    state_manager: Arc<RwLock<KlomangStateManager>>,
}

impl RpcServer {
    pub fn new(state_manager: Arc<RwLock<KlomangStateManager>>) -> Self {
        Self { state_manager }
    }

    /// Start RPC server
    pub async fn start(self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let state_manager = self.state_manager;

        // POST /eth_call
        let eth_call = warp::post()
            .and(warp::path("eth_call"))
            .and(warp::body::json())
            .and(with_state_manager(state_manager.clone()))
            .and_then(handle_eth_call);

        // POST /slug_estimateGas
        let estimate_gas = warp::post()
            .and(warp::path("slug_estimateGas"))
            .and(warp::body::json())
            .and(with_state_manager(state_manager.clone()))
            .and_then(handle_estimate_gas);

        // POST /send_transaction
        let send_tx = warp::post()
            .and(warp::path("send_transaction"))
            .and(warp::body::json())
            .and(with_state_manager(state_manager.clone()))
            .and_then(handle_send_transaction);

        let routes = eth_call.or(estimate_gas).or(send_tx);

        log::info!("Starting RPC server on port {}", port);
        warp::serve(routes).run(([127, 0, 0, 1], port)).await;

        Ok(())
    }
}

fn with_state_manager(
    state_manager: Arc<RwLock<KlomangStateManager>>,
) -> impl Filter<Extract = (Arc<RwLock<KlomangStateManager>>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || state_manager.clone())
}

async fn handle_eth_call(
    req: EthCallRequest,
    state_manager: Arc<RwLock<KlomangStateManager>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let sm = state_manager.read().unwrap();

    // Parse contract address
    let contract_addr = if let Some(addr_hex) = req.to {
        let hex_str = addr_hex.strip_prefix("0x").unwrap_or(&addr_hex);
        if let Ok(bytes) = hex::decode(hex_str) {
            if bytes.len() == 32 {
                let mut addr = [0u8; 32];
                addr.copy_from_slice(&bytes);
                Some(addr)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    // Parse call data
    let call_data = if req.data.starts_with("0x") {
        hex::decode(&req.data[2..])
    } else {
        hex::decode(&req.data)
    };

    let call_data = if let Ok(data) = call_data {
        data
    } else {
        return Ok(warp::reply::json(&RpcResponse::<CallResult> {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: None,
            error: Some(RpcError {
                code: -32602,
                message: "Invalid call data".to_string(),
            }),
        }));
    };

    // Execute call
    match sm.get_vm().call_contract(
        &contract_addr.unwrap_or_default(),
        &call_data,
        sm.get_verkle_tree(),
        req.gas_limit.unwrap_or(10_000_000),
    ) {
        Ok(result) => Ok(warp::reply::json(&RpcResponse {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: Some(CallResult {
                data: format!("0x{}", hex::encode(&result.return_data)),
                gas_used: result.gas_used,
            }),
            error: None,
        })),
        Err(e) => Ok(warp::reply::json(&RpcResponse::<CallResult> {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: None,
            error: Some(RpcError {
                code: -32000,
                message: format!("Execution error: {}", e),
            }),
        })),
    }
}

async fn handle_estimate_gas(
    req: EstimateGasRequest,
    state_manager: Arc<RwLock<KlomangStateManager>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let sm = state_manager.read().unwrap();

    let contract_addr = if let Some(addr_hex) = req.to {
        let hex_str = addr_hex.strip_prefix("0x").unwrap_or(&addr_hex);
        if let Ok(bytes) = hex::decode(hex_str) {
            if bytes.len() == 32 {
                let mut addr = [0u8; 32];
                addr.copy_from_slice(&bytes);
                Some(addr)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    let call_data = if req.data.starts_with("0x") {
        match hex::decode(&req.data[2..]) {
            Ok(data) => data,
            Err(_) => {
                return Ok(warp::reply::json(&RpcResponse::<CallResult> {
                    jsonrpc: "2.0".to_string(),
                    id: None,
                    result: None,
                    error: Some(RpcError {
                        code: -32602,
                        message: "Invalid call data".to_string(),
                    }),
                }));
            }
        }
    } else {
        match hex::decode(&req.data) {
            Ok(data) => data,
            Err(_) => {
                return Ok(warp::reply::json(&RpcResponse::<CallResult> {
                    jsonrpc: "2.0".to_string(),
                    id: None,
                    result: None,
                    error: Some(RpcError {
                        code: -32602,
                        message: "Invalid call data".to_string(),
                    }),
                }));
            }
        }
    };

    match sm.get_vm().dry_run_contract(
        &contract_addr.unwrap_or_default(),
        &call_data,
        sm.get_verkle_tree(),
        0,
        req.gas_limit.unwrap_or(10_000_000),
    ) {
        Ok(result) => Ok(warp::reply::json(&RpcResponse {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: Some(CallResult {
                data: format!("0x{}", hex::encode(&result.return_data)),
                gas_used: result.gas_used,
            }),
            error: None,
        })),
        Err(e) => Ok(warp::reply::json(&RpcResponse::<CallResult> {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: None,
            error: Some(RpcError {
                code: -32000,
                message: format!("Gas estimate error: {}", e),
            }),
        })),
    }
}

async fn handle_send_transaction(
    tx: SignedTransaction,
    state_manager: Arc<RwLock<KlomangStateManager>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let sm = state_manager.write().unwrap();

    match sm.add_transaction(tx) {
        Ok(tx_hash) => Ok(warp::reply::json(&RpcResponse {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: Some(format!("0x{}", hex::encode(tx_hash.as_bytes()))),
            error: None,
        })),
        Err(e) => Ok(warp::reply::json(&RpcResponse::<String> {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: None,
            error: Some(RpcError {
                code: -32000,
                message: format!("Transaction error: {}", e),
            }),
        })),
    }
}
