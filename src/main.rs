use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use chrono::{NaiveDateTime, Duration, Utc};
use warp::Filter;

// ==================== Data Structures ====================

#[derive(Debug, Serialize, Deserialize)]
struct Pair {
    liquidity: f64,
    CA: String,
    pair: String,
    gmgn_data: Value,
    gmgn_data_date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    transactions: Option<Vec<FilteredTransaction>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FilteredTransaction {
    time: String,
    price: String,
}

#[derive(Debug, Clone)]
struct DetailedTransaction {
    block_time: u64,
    filtered: FilteredTransaction,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TransactionDetails {
    block_time: u64,
    raw: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct SignatureInfo {
    signature: String,
    slot: u64,
    err: Option<Value>,
    memo: Option<String>,
    blockTime: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcResponse<T> {
    jsonrpc: String,
    result: T,
    id: u32,
}

struct TransactionSignature {
    signature: String,
}

// Custom error for Warp rejections
#[derive(Debug)]
struct ServerError(String);
impl warp::reject::Reject for ServerError {}

// ==================== RPC and Transaction Logic ====================

async fn get_signatures_for_address(
    address: &str,
    rpc_endpoint: &str,
    client: &Client,
    limit: usize,
    before: Option<&str>,
) -> Result<Vec<SignatureInfo>, Box<dyn Error + Send + Sync>> {
    let mut params = json!({ "limit": limit });
    if let Some(before_sig) = before {
        params["before"] = json!(before_sig);
    }
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [address, params],
    });

    let resp = client
        .post(rpc_endpoint)
        .json(&payload)
        .send()
        .await?
        .json::<RpcResponse<Vec<SignatureInfo>>>()
        .await?;

    Ok(resp.result)
}

fn filter_transaction(tx: &TransactionDetails, ca: &str) -> Option<FilteredTransaction> {
    let naive_time = NaiveDateTime::from_timestamp(tx.block_time as i64, 0);
    // In the original code, you had an option to adjust the time.
    let adjusted_time = naive_time;
    let formatted_time = adjusted_time.format("%d.%m.%Y %H:%M").to_string();

    if let Some(price) = compute_price(&tx.raw, ca) {
        let formatted_price = format!("{:.20}", price);
        Some(FilteredTransaction {
            time: formatted_time,
            price: formatted_price,
        })
    } else {
        println!("Failed to compute price for transaction at {}", formatted_time);
        None
    }
}

async fn get_transaction_details(
    signature: &TransactionSignature,
    rpc_endpoint: &str,
    client: &Client,
) -> Result<Option<TransactionDetails>, Box<dyn Error + Send + Sync>> {
    let tx_payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature.signature,
            { "maxSupportedTransactionVersion": 0, "encoding": "json" }
        ],
    });

    let tx_response = client
        .post(rpc_endpoint)
        .json(&tx_payload)
        .send()
        .await?
        .json::<Value>()
        .await?;

    if tx_response["result"].is_null() {
        return Ok(None);
    }
    let raw_tx = tx_response["result"].clone();
    let block_time = raw_tx["blockTime"].as_u64().unwrap_or(0);
    Ok(Some(TransactionDetails { block_time, raw: raw_tx }))
}

async fn fetch_signatures_last_24h(
    address: &str,
    rpc_endpoint: &str,
    client: &Client,
) -> Result<Vec<SignatureInfo>, Box<dyn Error + Send + Sync>> {
    let mut all_signatures = Vec::new();
    let mut before: Option<String> = None;
    let cutoff = Utc::now().timestamp() - 24 * 3600;

    loop {
        let batch = get_signatures_for_address(address, rpc_endpoint, client, 1000, before.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        let batch: Vec<SignatureInfo> = batch
            .into_iter()
            .filter(|sig| {
                if let Some(bt) = sig.blockTime {
                    (bt as i64) >= cutoff
                } else {
                    true
                }
            })
            .collect();

        if batch.is_empty() {
            break;
        }

        before = Some(batch.last().unwrap().signature.clone());
        all_signatures.extend(batch);
    }
    Ok(all_signatures)
}

fn compute_price(raw: &Value, ca: &str) -> Option<f64> {
    let meta = raw.get("meta")?;
    let post_token_balances = meta.get("postTokenBalances")?.as_array()?;
    let owner_filter = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1";
    let sol_mint = "So11111111111111111111111111111111111111112";

    let ca_total: f64 = post_token_balances.iter().filter_map(|entry| {
        let mint = entry.get("mint")?.as_str()?;
        let owner = entry.get("owner")?.as_str()?;
        if mint == ca && owner == owner_filter {
            entry.get("uiTokenAmount")?.get("uiAmount")?.as_f64()
        } else {
            None
        }
    }).sum();

    let sol_total: f64 = post_token_balances.iter().filter_map(|entry| {
        let mint = entry.get("mint")?.as_str()?;
        let owner = entry.get("owner")?.as_str()?;
        if mint == sol_mint && owner == owner_filter {
            entry.get("uiTokenAmount")?.get("uiAmount")?.as_f64()
        } else {
            None
        }
    }).sum();

    if ca_total > 0.0 {
        Some(sol_total / ca_total)
    } else {
        None
    }
}

fn detect_and_remove_anomalies(transactions: &mut Vec<FilteredTransaction>) {
    if transactions.len() <= 1 {
        return;
    }

    let prices: Vec<f64> = transactions
        .iter()
        .filter_map(|t| t.price.parse::<f64>().ok())
        .collect();
    let mean_price = prices.iter().sum::<f64>() / prices.len() as f64;
    let threshold = 0.1 * mean_price;

    transactions.retain(|t| {
        if let Ok(price) = t.price.parse::<f64>() {
            (price - mean_price).abs() <= threshold
        } else {
            false
        }
    });
}

async fn process_transactions(
    sigs: &[SignatureInfo],
    rpc_endpoint: &str,
    client: &Client,
    ca: &str,
) -> Result<Vec<FilteredTransaction>, Box<dyn Error + Send + Sync>> {
    let mut transactions = Vec::new();
    let mut processed_periods: HashMap<u64, Vec<FilteredTransaction>> = HashMap::new();
    
    let mut i = 0;
    while i < sigs.len() {
        let mut block = Vec::new();

        if let Some(sig) = sigs.get(i) {
            if let Some(block_time) = sig.blockTime {
                let period = block_time / 300; // Normalize to 5-minute intervals
                
                // Skip if this period is already processed
                if processed_periods.contains_key(&period) {
                    i += 1;
                    continue;
                }

                let mut count = 0;
                while count < 3 && i + count < sigs.len() {
                    if let Some(next_sig) = sigs.get(i + count) {
                        if let Some(next_block_time) = next_sig.blockTime {
                            if count > 0 && next_block_time >= block_time && (next_block_time - block_time) >= 60 {
                                break;
                            }
                            if let Some(tx) = get_transaction_details(
                                &TransactionSignature { signature: next_sig.signature.clone() },
                                rpc_endpoint,
                                client,
                            ).await? {
                                if let Some(filtered_tx) = filter_transaction(&tx, ca) {
                                    block.push(filtered_tx);
                                }
                            }
                        }
                    }
                    count += 1;
                }
                
                detect_and_remove_anomalies(&mut block);
                
                if let Some(selected_tx) = block.first().cloned() {
                    processed_periods.insert(period, vec![selected_tx.clone()]);
                    transactions.push(selected_tx);
                }
            }
        }
        i += 1;
    }

    println!("\n✅ Processing Complete! Total Blocks Processed: {}", processed_periods.len());
    Ok(transactions)
}

// ==================== HTTP Handler ====================

async fn process_pair_handler(mut pair: Pair) -> Result<impl warp::Reply, warp::Rejection> {
    // Define multiple RPC endpoints (cycling logic preserved)
    let rpc_endpoints = [
        "https://ultra-long-star.solana-mainnet.quiknode.pro/7a91feff2c7d87980e6d4094a4fd7b673f160331",
        "https://snowy-broken-replica.SOLANA-MAINNET.quiknode.pro/219d27e8ea90253468797a70427681f15729e358",
        "https://damp-withered-season.SOLANA-MAINNET.quiknode.pro/73c92f8dd067a5c53cdbd9925073598c52464d52",
    ];
    // For a single request, choose an endpoint based on (for example) index 0.
    let rpc_endpoint = rpc_endpoints[0];
    let client = Client::new();

    // Fetch signatures for the last 24 hours using the pair field (as address)
    let sigs = fetch_signatures_last_24h(&pair.pair, rpc_endpoint, &client)
        .await
        .map_err(|e| warp::reject::custom(ServerError(e.to_string())))?;
    
    let mut transactions = process_transactions(&sigs, rpc_endpoint, &client, &pair.CA)
        .await
        .map_err(|e| warp::reject::custom(ServerError(e.to_string())))?;
    
    // Adjust each "time" field by adding one hour
    for tx in &mut transactions {
        if let Ok(parsed_time) = NaiveDateTime::parse_from_str(&tx.time, "%d.%m.%Y %H:%M") {
            let adjusted_time = parsed_time + Duration::hours(1);
            tx.time = adjusted_time.format("%d.%m.%Y %H:%M").to_string();
        }
    }
    
    // Sort transactions from oldest to newest
    transactions.sort_by(|a, b| a.time.cmp(&b.time));
    
    // Remove duplicate transactions based on "time"
    let mut unique_transactions = Vec::new();
    let mut seen_times = HashSet::new();
    for tx in transactions {
        if seen_times.insert(tx.time.clone()) {
            unique_transactions.push(tx);
        }
    }
    
    pair.transactions = Some(unique_transactions);
    
    Ok(warp::reply::json(&pair))
}

// ==================== Main Server ====================

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let process_route = warp::post()
        .and(warp::path("process"))
        .and(warp::body::json())
        .and_then(process_pair_handler);

    // Fetch the port from the environment variable (default to 3030 if not set)
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3030".to_string())
        .parse()
        .expect("PORT must be a valid u16 number");

    let addr = ([0, 0, 0, 0], port);

    println!("🚀 Server starting on http://0.0.0.0:{}", port);
    warp::serve(process_route).run(addr).await;
    Ok(())
}
