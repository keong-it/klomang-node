mod ingestion_guard;
mod mempool;
mod network;
mod rpc;
mod state;
mod storage;
mod vm;

use crate::ingestion_guard::{IngestionMessage, RateLimiter, create_ingestion_queue};
use crate::state::ingestion::{IngestionReceiver, IngestionSender};
use clap::Parser;
use env_logger;
use klomang_core::{Dag, UtxoSet};
use log::info;
use std::collections::HashSet;
use std::error::Error;
use std::sync::{Arc, RwLock};
use storage::{ChainIndexRecord, KlomangStorage, StorageHandle};

#[derive(Parser)]
#[command(name = "klomang-node")]
#[command(about = "Klomang Node CLI", long_about = None)]
struct CliArgs {
    #[arg(long, default_value_t = 3833)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let args = CliArgs::parse();
    info!("Starting Klomang Node on port {}", args.port);

    println!("Klomang Node v0.1.0");

    let dag = Dag::new();
    let utxo = UtxoSet::new();

    info!(
        "Initialized klomang-core Dag ({} blocks) and UtxoSet",
        dag.get_block_count()
    );
    println!(
        "klomang-core loaded: Dag count={} UtxoSet entries={}",
        dag.get_block_count(),
        utxo.utxos.len()
    );

    // Open storage dengan automatic recovery jika database corrupt
    // Konfigurasi mode pruning via env: KLPM_PRUNING_MODE=archive|pruned
    let pruning_mode = match std::env::var("KLPM_PRUNING_MODE").as_deref() {
        Ok("archive") => storage::config::PruningStrategy::Archive,
        _ => storage::config::PruningStrategy::Pruned(1000),
    };
    let storage: StorageHandle =
        KlomangStorage::open_with_recovery("./data/klomang_db", pruning_mode)?.into_handle();

    // Initialize State Manager - central orchestration component
    let state_manager = match state::KlomangStateManager::new(storage.clone()) {
        Ok(sm) => {
            println!("✅ State Manager initialized successfully");
            println!("   Best block: {}", sm.get_best_block().to_hex());
            Arc::new(RwLock::new(sm))
        }
        Err(e) => {
            println!("❌ Failed to initialize State Manager: {:?}", e);
            return Err(e.into());
        }
    };

    // Start mempool background tasks
    {
        let sm = state_manager.write().unwrap();
        sm.start_mempool_tasks()?;
    }
    println!("✅ Mempool background tasks started");

    // Initialize Ingestion Guard
    let (ingestion_sender, mut ingestion_receiver, rate_limiter) =
        create_ingestion_queue(10_000, 1000);
    println!("✅ Ingestion queue initialized with capacity 10,000 and rate limit 1000 blocks/sec");

    // Initialize Network Manager - P2P communication layer
    let network_config = network::NetworkConfig::default();
    let mut network_manager = match network::NetworkManager::new(
        storage.clone(),
        network_config,
        ingestion_sender.clone(),
        state_manager.clone(),
    )
    .await
    {
        Ok(nm) => {
            println!("✅ Network Manager initialized successfully");
            println!("   Local PeerID: {}", nm.local_peer_id());
            nm
        }
        Err(e) => {
            println!("❌ Failed to initialize Network Manager: {:?}", e);
            return Err(e.into());
        }
    };

    // Spawn ingestion worker
    let state_manager_clone = state_manager.clone();
    tokio::spawn(async move {
        while let Some(msg) = ingestion_receiver.recv().await {
            match msg {
                IngestionMessage::Block(block) => {
                    log::info!("[INGESTION] Processing block {}", block.header.id);
                    // Process block through state manager
                    if let Err(e) = state_manager_clone
                        .write()
                        .map_err(|e| format!("State manager lock poisoned: {}", e))
                        .unwrap()
                        .process_block(block)
                    {
                        log::error!("[INGESTION] Failed to process block: {}", e);
                    }
                }
                IngestionMessage::Shutdown => {
                    log::info!("[INGESTION] Shutdown signal received");
                    break;
                }
            }
        }
    });

    // Spawn network event loop
    tokio::spawn(async move {
        if let Err(e) = network_manager.run().await {
            log::error!("Network error: {}", e);
        }
    });

    // Start RPC server
    let rpc_server = rpc::RpcServer::new(state_manager.clone());
    tokio::spawn(async move {
        if let Err(e) = rpc_server.start(8545).await {
            log::error!("RPC server error: {}", e);
        }
    });
    println!("✅ RPC server started on port 8545");

    // TODO: Uncomment test code when BlockHeader is available
    /*
    let genesis_id = Hash::new(b"genesis-block");
    let genesis_block = BlockNode {
        header: BlockHeader {
            id: genesis_id.clone(),
            parents: HashSet::new(),
            timestamp: 0,
            difficulty: 1,
            nonce: 0,
            verkle_root: Hash::new(b"genesis-verkle-root"),
            verkle_proofs: None,
            signature: None,
        },
        children: HashSet::new(),
        selected_parent: None,
        blue_set: HashSet::new(),
        red_set: HashSet::new(),
        blue_score: 0,
        transactions: Vec::new(),
    };

    let chain_index = ChainIndexRecord {
        height: 0,
        tip: genesis_id.clone(),
        total_work: 0,
    };

    let txs: Vec<Transaction> = Vec::new();
    storage.write().map_err(|e| format!("Storage write lock poisoned: {}", e))?.save_block_atomic(&genesis_block, &txs, &chain_index)?;

    if let Some(record) = storage.read().map_err(|e| format!("Storage read lock poisoned: {}", e))?.get_chain_index(&genesis_id)? {
        println!("Chain index loaded from storage: height={} tip={} total_work={}", record.height, record.tip.to_hex(), record.total_work);
    }

    // Test bulk batch operations untuk IBD simulation
    let mut batch_blocks = Vec::new();
    for i in 1..=10 {
        let block_id = Hash::new(format!("block-{}", i).as_bytes());
        let block = BlockNode {
            header: BlockHeader {
                id: block_id.clone(),
                parents: HashSet::from([genesis_id.clone()]),
                timestamp: i as u64 * 1000,
                difficulty: 1,
                nonce: i as u64,
                verkle_root: Hash::new(format!("verkle-root-{}", i).as_bytes()),
                verkle_proofs: None,
                signature: None,
            },
            children: HashSet::new(),
            selected_parent: Some(genesis_id.clone()),
            blue_set: HashSet::new(),
            red_set: HashSet::new(),
            blue_score: i as u64,
            transactions: Vec::new(),
        };

        let index = ChainIndexRecord {
            height: i as u64,
            tip: block_id.clone(),
            total_work: i as u128,
        };

        batch_blocks.push((block, Vec::new(), index));
    }

    // Save batch atomically dengan write lock
    let (count, throughput) = storage.write().map_err(|e| format!("Storage write lock poisoned: {}", e))?.save_block_batch(batch_blocks)?;
    println!("Bulk batch saved: {} blocks at {} blocks/sec", count, throughput);

    // Test Pruning System - keep last 5 blocks, delete older ones
    info!("Testing pruning system: keeping last 5 blocks");
    let (deleted_blocks, freed_space) = storage.write().map_err(|e| format!("Storage write lock poisoned: {}", e))?.run_pruning(5)?;
    println!("Pruning result: deleted {} blocks, freed {} MB", deleted_blocks, freed_space / (1024 * 1024));

    // Display storage stats dengan read lock
    if let Ok(stats) = storage.read().map_err(|e| format!("Storage read lock poisoned: {}", e))?.get_storage_stats() {
        println!(
            "Storage stats - SST Files: {}, DB Size: {} MB",
            stats.num_sst_files,
            stats.db_size_bytes / (1024 * 1024)
        );
    }
    // Demonstrate State Manager block processing
    println!("\n🧠 Testing State Manager block processing...");
    let test_block = BlockNode {
        header: BlockHeader {
            id: Hash::new(b"test-block"),
            parents: std::collections::HashSet::from([genesis_id.clone()]),
            timestamp: 1000,
            difficulty: 1,
            nonce: 42,
            verkle_root: Hash::new(b"test-verkle-root"),
            verkle_proofs: None,
            signature: None,
        },
        children: std::collections::HashSet::new(),
        selected_parent: Some(genesis_id.clone()),
        blue_set: std::collections::HashSet::new(),
        red_set: std::collections::HashSet::new(),
        blue_score: 1,
        transactions: Vec::new(),
    };

    match state_manager.write().map_err(|e| format!("State manager write lock poisoned: {}", e))?.process_block(test_block) {
        Ok(()) => println!("✅ Block processed successfully by State Manager"),
        Err(e) => println!("❌ Block processing failed: {:?}", e),
    }
    */

    // Keep the node running
    println!("\n🚀 Klomang Node is running... Press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    // Graceful shutdown
    info!("Preparing graceful shutdown");
    storage
        .read()
        .map_err(|e| format!("Storage read lock poisoned: {}", e))?
        .shutdown()?;
    info!("Klomang Node shutdown completed successfully");

    Ok(())
}
