// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use moltendb_core::engine::{Db, DbConfig};
use sysinfo::{Disks, System};
use moltendb_core::handlers::{process_set, process_get, process_stats};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tauri::{AppHandle, Emitter, State};

// ── Synthetic data generation (mirrors generate_stress_data.rs) ───────────────

const BRANDS: &[&str] = &["Lenovo", "Apple", "Asus", "Dell", "Razer", "Framework", "HP", "Acer"];
const PANELS: &[&str] = &["IPS", "OLED", "Mini-LED", "VA", "TN"];
const TAGS: &[&[&str]] = &[
    &["business", "ultrabook", "lightweight"],
    &["creative", "professional", "macos"],
    &["gaming", "amd", "portable"],
    &["gaming", "windows", "rgb"],
    &["modular", "linux", "budget"],
    &["creative", "windows", "4k"],
    &["student", "budget", "lightweight"],
    &["workstation", "professional", "windows"],
];

fn synthetic_entry(i: usize) -> Value {
    let brand = BRANDS[i % BRANDS.len()];
    let model = format!("Model-{:05}", i);
    let price = 499 + (i % 3001) as u64;
    let in_stock = i % 3 != 0;
    let cores = 4 + (i % 13) as u64;
    let ghz = 2.0 + (i % 30) as f64 * 0.1;
    let battery = 40 + (i % 61) as u64;
    let weight = 1.0 + (i % 15) as f64 * 0.1;
    let panel = PANELS[i % PANELS.len()];
    let size = 13.0 + (i % 5) as f64 * 0.5;
    let refresh = [60u64, 90, 120, 144, 165][i % 5];
    let mem_gb = [8u64, 16, 32, 64][(i / 4) % 4];
    let tags: Vec<&str> = TAGS[i % TAGS.len()].to_vec();

    json!({
        "brand": brand,
        "model": model,
        "price": price,
        "in_stock": in_stock,
        "tags": tags,
        "specs": {
            "cpu": { "brand": brand, "cores": cores, "ghz": ghz },
            "battery_wh": battery,
            "weight_kg": weight
        },
        "display": {
            "size_inch": size,
            "panel": panel,
            "refresh_hz": refresh,
            "hdr": refresh >= 120
        },
        "memory": {
            "capacity_gb": mem_gb,
            "type": if mem_gb <= 16 { "LPDDR5" } else { "DDR5" }
        }
    })
}

// ── Shared DB state ───────────────────────────────────────────────────────────

struct DbState(Mutex<Option<Arc<Db>>>);

// ── Result types ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct QueryResult {
    name: String,
    elapsed_ms: f64,
    doc_count: usize,
    result: Value,
}

#[derive(Serialize, Deserialize)]
struct InsertResult {
    storage_mode: String,
    doc_count: usize,
    insert_ms: f64,
    insert_docs_per_sec: f64,
}

// ── Tauri commands ────────────────────────────────────────────────────────────

fn emit_progress(app: &AppHandle, stage: &str, message: &str, pct: u8) {
    let _ = app.emit("benchmark-progress", json!({
        "stage": stage,
        "message": message,
        "pct": pct
    }));
}

/// Insert synthetic data into the DB and keep it open for subsequent queries.
#[tauri::command]
async fn insert_data(
    app: AppHandle,
    db_state: State<'_, DbState>,
    storage_mode: String,
    doc_count: usize,
    db_path: String,
) -> Result<InsertResult, String> {
    let app2 = app.clone();
    // We need to move db_state inner out — clone the Arc<Mutex<...>>
    let state_inner = db_state.0.lock().map_err(|e| e.to_string())?;
    drop(state_inner); // release lock before spawn_blocking

    let db_state_arc = db_state.inner() as *const DbState as usize; // raw ptr trick for Send

    tokio::task::spawn_blocking(move || {
        // SAFETY: DbState lives for the entire app lifetime (managed state).
        let db_state_ref = unsafe { &*(db_state_arc as *const DbState) };
        insert_data_sync(app2, db_state_ref, storage_mode, doc_count, db_path)
    })
    .await
    .map_err(|e| format!("Task panicked: {:?}", e))?
}

fn insert_data_sync(
    app: AppHandle,
    db_state: &DbState,
    storage_mode: String,
    doc_count: usize,
    db_path: String,
) -> Result<InsertResult, String> {
    let config = match storage_mode.as_str() {
        "inMemory" => DbConfig {
            in_memory: true,
            max_body_size: 300 * 1024 * 1024,
            max_keys_per_request: 100_000,
            rate_limit_requests: Some(100000),
            ..DbConfig::default()
        },
        "sync" => DbConfig {
            in_memory: false,
            max_body_size: 300 * 1024 * 1024,
            max_keys_per_request: 100_000,
            path: db_path.clone(),
            rate_limit_requests: Some(100000),
            sync_mode: true,
            ..DbConfig::default()
        },
        "async" => DbConfig {
            in_memory: false,
            max_body_size: 300 * 1024 * 1024,
            max_keys_per_request: 100_000,
            path: db_path.clone(),
            rate_limit_requests: Some(100000),
            sync_mode: false,
            ..DbConfig::default()
        },
        other => return Err(format!("Unknown storage mode: {}", other)),
    };

    emit_progress(&app, "open", "Opening database…", 0);
    let db = Db::open(config).map_err(|e| format!("Failed to open DB: {:?}", e))?;
    let actual_mode = db.storage.storage_mode().to_string();

    let batch_size = 15_000.min(doc_count);
    let insert_start = Instant::now();
    let mut inserted = 0usize;
    let num_batches = (doc_count + batch_size - 1) / batch_size;
    let mut batch_idx = 0usize;

    while inserted < doc_count {
        let chunk = batch_size.min(doc_count - inserted);
        let pct = (batch_idx * 98 / num_batches.max(1)) as u8;
        emit_progress(
            &app,
            "insert",
            &format!("Inserting docs {}/{}…", inserted + chunk, doc_count),
            pct,
        );
        let mut data = serde_json::Map::new();
        for j in 0..chunk {
            let i = inserted + j;
            let key = format!("stress_{:06}", i);
            data.insert(key, synthetic_entry(i));
        }
        let payload = json!({
            "collection": "stress",
            "data": Value::Object(data)
        });
        let (code, body) = process_set(&db, &payload, usize::MAX, usize::MAX);
        if code != 200 {
            return Err(format!("Insert batch failed (HTTP {}): {}", code, body));
        }
        inserted += chunk;
        batch_idx += 1;
    }

    let insert_elapsed = insert_start.elapsed().as_secs_f64() * 1000.0;
    let insert_docs_per_sec = doc_count as f64 / (insert_elapsed / 1000.0);

    emit_progress(
        &app,
        "done",
        &format!("Insert complete — {:.1}ms ({:.0} docs/s)", insert_elapsed, insert_docs_per_sec),
        100,
    );

    // Store the open DB for later queries.
    let mut lock = db_state.0.lock().map_err(|e| e.to_string())?;
    *lock = Some(Arc::new(db));

    Ok(InsertResult {
        storage_mode: actual_mode,
        doc_count,
        insert_ms: insert_elapsed,
        insert_docs_per_sec,
    })
}

/// Run a single named query against the currently loaded DB.
/// `query_id` is one of the predefined query identifiers (e.g. "§1a", "§3b", …).
/// `doc_count` is needed to compute dynamic keys (mid-range, 3/4 range).
#[tauri::command]
async fn run_query(
    db_state: State<'_, DbState>,
    query_id: String,
    doc_count: usize,
) -> Result<QueryResult, String> {
    let db_arc = {
        let lock = db_state.0.lock().map_err(|e| e.to_string())?;
        lock.clone().ok_or_else(|| "No database loaded. Please insert data first.".to_string())?
    };

    tokio::task::spawn_blocking(move || run_query_sync(db_arc, query_id, doc_count))
        .await
        .map_err(|e| format!("Task panicked: {:?}", e))?
}

/// Run a custom query given a raw JSON payload string.
#[tauri::command]
async fn run_custom_query(
    db_state: State<'_, DbState>,
    name: String,
    payload_json: String,
) -> Result<QueryResult, String> {
    let db_arc = {
        let lock = db_state.0.lock().map_err(|e| e.to_string())?;
        lock.clone().ok_or_else(|| "No database loaded. Please insert data first.".to_string())?
    };
    tokio::task::spawn_blocking(move || {
        let payload: Value = serde_json::from_str(&payload_json)
            .map_err(|e| format!("Invalid JSON: {}", e))?;
        let t0 = Instant::now();
        let (_, body) = process_get(&db_arc, &payload, usize::MAX, usize::MAX);
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let doc_count_result = count_docs(&body);
        Ok(QueryResult { name, elapsed_ms, doc_count: doc_count_result, result: body })
    })
    .await
    .map_err(|e| format!("Task panicked: {:?}", e))?
}

/// Get DB stats (collection counts + system info).
#[tauri::command]
async fn get_stats(
    db_state: State<'_, DbState>,
) -> Result<Value, String> {
    let db_arc = {
        let lock = db_state.0.lock().map_err(|e| e.to_string())?;
        match lock.clone() {
            Some(db) => db,
            None => return Ok(json!({ "status": "no_db", "message": "No database loaded yet" })),
        }
    };
    tokio::task::spawn_blocking(move || {
        let (_, stats_body) = process_stats(&db_arc, &json!({}));
        let uptime_secs = db_arc.started_at.elapsed().as_secs();
        let hot_keys = db_arc.hot_keys_count();
        let storage_mode = db_arc.storage.storage_mode();
        Ok(json!({
            "stats": stats_body,
            "uptime_secs": uptime_secs,
            "hot_keys": hot_keys,
            "storage_mode": storage_mode,
        }))
    })
    .await
    .map_err(|e| format!("Task panicked: {:?}", e))?
}

fn run_query_sync(db: Arc<Db>, query_id: String, doc_count: usize) -> Result<QueryResult, String> {
    let max_body = usize::MAX;
    let max_keys = usize::MAX;

    let (name, payload) = build_query(&query_id, doc_count)
        .ok_or_else(|| format!("Unknown query id: {}", query_id))?;

    let t0 = Instant::now();
    let (_, body) = process_get(&db, &payload, max_body, max_keys);
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let doc_count_result = count_docs(&body);

    Ok(QueryResult {
        name: name.to_string(),
        elapsed_ms,
        doc_count: doc_count_result,
        result: body,
    })
}

fn build_query(query_id: &str, doc_count: usize) -> Option<(&'static str, Value)> {
    let mid = format!("stress_{:06}", doc_count / 2);
    let three_q = format!("stress_{:06}", doc_count * 3 / 4);
    let quarter = format!("stress_{:06}", doc_count / 4);

    let result = match query_id {
        // §1 — Point lookups
        "§1a" => ("§1a Point lookup: stress_000060", json!({
            "collection": "stress", "keys": "stress_000060"
        })),
        "§1b" => ("§1b Point lookup: mid-range key", json!({
            "collection": "stress", "keys": mid
        })),
        "§1c" => ("§1c Batch fetch: 3 keys", json!({
            "collection": "stress",
            "keys": ["stress_000000", quarter, three_q]
        })),

        // §2 — Field projection
        "§2a" => ("§2a Projection: brand, model, price", json!({
            "collection": "stress",
            "keys": ["stress_000000", "stress_000001", "stress_000002"],
            "fields": ["brand", "model", "price"]
        })),
        "§2b" => ("§2b Projection: nested CPU specs", json!({
            "collection": "stress",
            "keys": ["stress_000000", "stress_000001", "stress_000002"],
            "fields": ["brand", "model", "specs.cpu.brand", "specs.cpu.cores", "specs.cpu.ghz"]
        })),

        // §3 — Simple WHERE filters
        "§3a" => ("§3a WHERE brand = Apple (~12.5%)", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "where": { "brand": { "$eq": "Apple" } }
        })),
        "§3b" => ("§3b WHERE in_stock AND price < 1000", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price", "in_stock"],
            "where": { "in_stock": true, "price": { "$lt": 1000 } }
        })),
        "§3c" => ("§3c WHERE price BETWEEN 1500 AND 2500", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "where": { "price": { "$gte": 1500, "$lte": 2500 } }
        })),
        "§3d" => ("§3d WHERE cpu.brand != Intel", json!({
            "collection": "stress",
            "fields": ["brand", "model", "specs.cpu.brand"],
            "where": { "specs.cpu.brand": { "$ne": "Intel" } }
        })),
        "§3e" => ("§3e WHERE cpu.cores >= 8", json!({
            "collection": "stress",
            "fields": ["brand", "model", "specs.cpu.cores"],
            "where": { "specs.cpu.cores": { "$gte": 8 } }
        })),
        "§3f" => ("§3f WHERE tags contains 'gaming'", json!({
            "collection": "stress",
            "fields": ["brand", "model", "tags", "price"],
            "where": { "tags": { "$ct": "gaming" } }
        })),
        "§3g" => ("§3g WHERE display.panel = OLED", json!({
            "collection": "stress",
            "fields": ["brand", "model", "display.panel", "price"],
            "where": { "display.panel": { "$eq": "OLED" } }
        })),
        "§3i" => ("§3i WHERE brand IN [Apple, Dell, Razer]", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "where": { "brand": { "$in": ["Apple", "Dell", "Razer"] } }
        })),
        "§3j" => ("§3j WHERE brand NIN [Framework, Lenovo]", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "where": { "brand": { "$nin": ["Framework", "Lenovo"] } }
        })),

        // §4 — Logical operators
        "§4a" => ("§4a $or: Apple OR gaming tag", json!({
            "collection": "stress",
            "fields": ["brand", "model", "tags"],
            "where": {
                "$or": [
                    { "brand": { "$eq": "Apple" } },
                    { "tags": { "$ct": "gaming" } }
                ]
            }
        })),
        "§4b" => ("§4b $and: in_stock AND price<1500 AND cores>=8", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price", "specs.cpu.cores"],
            "where": {
                "$and": [
                    { "in_stock": true },
                    { "price": { "$lt": 1500 } },
                    { "specs.cpu.cores": { "$gte": 8 } }
                ]
            }
        })),
        "§4c" => ("§4c $or + top-level: in_stock AND (Apple OR Dell)", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "where": {
                "in_stock": true,
                "$or": [
                    { "brand": { "$eq": "Apple" } },
                    { "brand": { "$eq": "Dell" } }
                ]
            }
        })),
        "§4d" => ("§4d $or nested: Mini-LED OR OLED display", json!({
            "collection": "stress",
            "fields": ["brand", "model", "display.panel", "price"],
            "where": {
                "$or": [
                    { "display.panel": { "$eq": "Mini-LED" } },
                    { "display.panel": { "$eq": "OLED" } }
                ]
            }
        })),

        // §5 — Sort
        "§5a" => ("§5a Sort: Apple by price asc", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "where": { "brand": { "$eq": "Apple" } },
            "sort": [{ "field": "price", "order": "asc" }]
        })),
        "§5c" => ("§5c Multi-sort: brand asc, price asc", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "sort": [
                { "field": "brand", "order": "asc" },
                { "field": "price", "order": "asc" }
            ]
        })),
        "§5d" => ("§5d Sort: cpu.cores desc", json!({
            "collection": "stress",
            "fields": ["brand", "model", "specs.cpu.cores"],
            "sort": [{ "field": "specs.cpu.cores", "order": "desc" }]
        })),

        // §6 — Pagination
        "§6a" => ("§6a Top 10 cheapest", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "sort": [{ "field": "price", "order": "asc" }],
            "count": 10
        })),
        "§6b" => ("§6b Page 2 cheapest (skip 10, take 10)", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price"],
            "sort": [{ "field": "price", "order": "asc" }],
            "offset": 10,
            "count": 10
        })),
        "§6d" => ("§6d Top 5 most expensive gaming items", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price", "tags"],
            "where": { "tags": { "$ct": "gaming" } },
            "sort": [{ "field": "price", "order": "desc" }],
            "count": 5
        })),

        // §7 — Combined WHERE + sort + pagination
        "§7a" => ("§7a In-stock OLED, price asc, top 10", json!({
            "collection": "stress",
            "fields": ["brand", "model", "price", "display.panel"],
            "where": { "in_stock": true, "display.panel": { "$eq": "OLED" } },
            "sort": [{ "field": "price", "order": "asc" }],
            "count": 10
        })),
        "§7b" => ("§7b Non-Apple/Lenovo, 120Hz+, sort refresh desc, top 20", json!({
            "collection": "stress",
            "fields": ["brand", "model", "display.refresh_hz", "price"],
            "where": {
                "brand": { "$nin": ["Apple", "Lenovo"] },
                "display.refresh_hz": { "$gte": 120 }
            },
            "sort": [{ "field": "display.refresh_hz", "order": "desc" }],
            "count": 20
        })),
        "§7c" => ("§7c Lightweight in-stock (<1.5kg), sort weight asc, top 10", json!({
            "collection": "stress",
            "fields": ["brand", "model", "specs.weight_kg", "price"],
            "where": { "in_stock": true, "specs.weight_kg": { "$lt": 1.5 } },
            "sort": [{ "field": "specs.weight_kg", "order": "asc" }],
            "count": 10
        })),

        _ => return None,
    };
    Some(result)
}

/// Get real-time system metrics: CPU, RAM, disk.
#[tauri::command]
async fn get_metrics() -> Result<Value, String> {
    tokio::task::spawn_blocking(|| {
        let mut sys = System::new_all();
        sys.refresh_all();
        // Brief sleep so CPU usage delta is meaningful
        std::thread::sleep(std::time::Duration::from_millis(200));
        sys.refresh_all();

        // Per-CPU usage
        let cpus: Vec<Value> = sys.cpus().iter().enumerate().map(|(i, c)| {
            json!({ "id": i, "usage": (c.cpu_usage() * 10.0).round() / 10.0, "freq_mhz": c.frequency() })
        }).collect();
        let avg_cpu = if cpus.is_empty() { 0.0 } else {
            let sum: f32 = sys.cpus().iter().map(|c| c.cpu_usage()).sum();
            (sum / sys.cpus().len() as f32 * 10.0).round() / 10.0
        };

        let total_ram = sys.total_memory();
        let used_ram  = sys.used_memory();
        let free_ram  = sys.free_memory();

        let pid = sysinfo::get_current_pid().ok();
        let process_mem = pid
            .and_then(|p| sys.process(p))
            .map(|p| p.memory())
            .unwrap_or(0);

        let disks = Disks::new_with_refreshed_list();
        let disk_info: Vec<Value> = disks.iter().map(|d| {
            let total = d.total_space();
            let avail = d.available_space();
            let used  = total.saturating_sub(avail);
            let pct   = if total > 0 { (used as f64 / total as f64 * 1000.0).round() / 10.0 } else { 0.0 };
            json!({
                "mount":       d.mount_point().to_string_lossy(),
                "total_gb":    (total as f64 / 1_073_741_824.0 * 10.0).round() / 10.0,
                "used_gb":     (used  as f64 / 1_073_741_824.0 * 10.0).round() / 10.0,
                "avail_gb":    (avail as f64 / 1_073_741_824.0 * 10.0).round() / 10.0,
                "used_pct":    pct,
            })
        }).collect();

        Ok(json!({
            "cpu": {
                "avg_pct":  avg_cpu,
                "cores":    cpus,
                "count":    sys.cpus().len(),
            },
            "ram": {
                "total_gb": (total_ram as f64 / 1_073_741_824.0 * 100.0).round() / 100.0,
                "used_gb":  (used_ram  as f64 / 1_073_741_824.0 * 100.0).round() / 100.0,
                "free_gb":  (free_ram  as f64 / 1_073_741_824.0 * 100.0).round() / 100.0,
                "used_pct": if total_ram > 0 { (used_ram as f64 / total_ram as f64 * 1000.0).round() / 10.0 } else { 0.0 },
                "process_mb": (process_mem as f64 / 1_048_576.0 * 10.0).round() / 10.0,
            },
            "disks": disk_info,
        }))
    })
    .await
    .map_err(|e| format!("Task panicked: {:?}", e))?
}

/// Count documents returned in a process_get response body.
fn count_docs(body: &Value) -> usize {
    match body {
        Value::Array(arr) => arr.len(),
        Value::Object(obj) => {
            // Error responses have an "error" key — count as 0.
            if obj.contains_key("error") { 0 } else { 1 }
        }
        _ => 0,
    }
}

// ── App entry point ───────────────────────────────────────────────────────────

fn main() {
    tauri::Builder::default()
        .manage(DbState(Mutex::new(None)))
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![insert_data, run_query, run_custom_query, get_stats, get_metrics])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
