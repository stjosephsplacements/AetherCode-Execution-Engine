//! `shadowdiff --redis <url> --pg <url> [--since <dur>]`: exit 1 when any shadow
//! record differs from the Go engine's result, 0 otherwise.

use ae_shadowdiff::{Outcome, ShadowRecord, Since, Summary, compare, since_filter};
use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "Compare ac:shadow:<job_id> records with the Go engine's Postgres results")]
struct Args {
    /// Redis URL, e.g. redis://127.0.0.1:6379/ (or env SHADOWDIFF_REDIS). Command-line
    /// flags are visible in `ps`; prefer the environment variable when it holds a password.
    #[arg(long, env = "SHADOWDIFF_REDIS", hide_env_values = true)]
    redis: String,
    /// Postgres URL of the authoritative engine's database (or env SHADOWDIFF_PG, then
    /// AC_DATABASE_URL). Must connect as the owner role or one that bypasses row-level
    /// security: under RLS every submission looks missing and is reported as pending.
    #[arg(long, env = "SHADOWDIFF_PG", hide_env_values = true)]
    pg: Option<String>,
    /// Exit 1 when any job has no finished Go result yet (and list the job ids).
    #[arg(long)]
    fail_on_pending: bool,
    /// Fail (exit 1) when fewer than this many records were compared, so an empty or
    /// mis-pointed run cannot report parity.
    #[arg(long, default_value_t = 1)]
    min_compared: usize,
    /// Accept a run that compared nothing (same as --min-compared 0).
    #[arg(long)]
    allow_empty: bool,
    /// Only records written within this long ago (e.g. 30m, 1h).
    #[arg(long, value_parser = humantime::parse_duration)]
    since: Option<Duration>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(e) => {
            eprintln!("shadowdiff: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run(a: Args) -> Result<bool, String> {
    let client = redis::Client::open(a.redis.as_str()).map_err(|e| format!("redis url: {e}"))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("redis connect: {e}"))?;
    let pg =
        a.pg.or_else(|| std::env::var("AC_DATABASE_URL").ok())
            .filter(|u| !u.is_empty())
            .ok_or("no Postgres URL: pass --pg or set SHADOWDIFF_PG / AC_DATABASE_URL")?;
    let cfg = ae_core::Config::from_map(&HashMap::from([("AC_DATABASE_URL".to_owned(), pg)]))
        .map_err(|e| format!("config: {e}"))?;
    let db = ae_db::connect(&cfg)
        .await
        .map_err(|e| format!("postgres connect: {e}"))?;

    let cutoff = a.since.map(|d| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |t| t.as_millis());
        i64::try_from(now.saturating_sub(d.as_millis())).unwrap_or(0)
    });

    let mut sum = Summary::default();
    let mut seen = HashSet::new();
    let mut pending_ids = Vec::new();
    let mut unreadable_ids = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("ac:shadow:*")
            .arg("COUNT")
            .arg(500)
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("redis scan: {e}"))?;
        for key in keys {
            // SCAN may return a key more than once.
            if !seen.insert(key.clone()) {
                continue;
            }
            let body: Option<String> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("redis get {key}: {e}"))?;
            let Some(body) = body else { continue };
            let rec: ShadowRecord = match serde_json::from_str(&body) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("{key}: unreadable record: {e}");
                    sum.unreadable += 1;
                    unreadable_ids.push(key.trim_start_matches("ac:shadow:").to_owned());
                    continue;
                }
            };
            match since_filter(&rec, cutoff) {
                Since::Keep => {}
                Since::Skip => {
                    sum.skipped_since += 1;
                    continue;
                }
                Since::Unreadable => {
                    eprintln!("{key}: missing recorded_at, cannot apply --since");
                    sum.unreadable += 1;
                    unreadable_ids.push(rec.job_id.clone());
                    continue;
                }
            }
            let sub = db
                .get_submission(&rec.job_id)
                .await
                .map_err(|e| format!("postgres {}: {e}", rec.job_id))?;
            let go = if sub.is_some() {
                db.get_test_results(&rec.job_id)
                    .await
                    .map_err(|e| format!("postgres results {}: {e}", rec.job_id))?
            } else {
                Vec::new()
            };
            let out = compare(&rec, sub.as_ref(), &go);
            if let Outcome::Diff(lines) = &out {
                println!("DIFF {}", rec.job_id);
                for l in lines {
                    println!("  {l}");
                }
            }
            if out == Outcome::Pending {
                pending_ids.push(rec.job_id.clone());
            }
            sum.add(&out);
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    for id in &unreadable_ids {
        println!("UNREADABLE {id}");
    }
    let min = if a.allow_empty { 0 } else { a.min_compared };
    let failed = sum.failed(a.fail_on_pending, min);
    if sum.too_few(min) {
        println!(
            "no records compared (compared={}, required {min}); refusing to report parity",
            sum.compared
        );
    }
    if sum.pending > 0 && (a.fail_on_pending || sum.compared == 0) {
        for id in &pending_ids {
            println!("PENDING {id}");
        }
    }
    println!("{}", sum.line());
    Ok(failed)
}
