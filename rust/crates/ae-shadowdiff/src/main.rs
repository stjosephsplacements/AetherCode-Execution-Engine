//! `shadowdiff --redis <url> --pg <url> [--since <dur>]`: exit 1 when any shadow
//! record differs from the Go engine's result, 0 otherwise.

use ae_shadowdiff::{Outcome, ShadowRecord, Summary, compare};
use clap::Parser;
use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "Compare ac:shadow:<job_id> records with the Go engine's Postgres results")]
struct Args {
    /// Redis URL, e.g. redis://127.0.0.1:6379/
    #[arg(long)]
    redis: String,
    /// Postgres URL of the authoritative engine's database.
    #[arg(long)]
    pg: String,
    /// Only records written within this long ago (e.g. 30m, 1h).
    #[arg(long, value_parser = humantime::parse_duration)]
    since: Option<Duration>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(0) => ExitCode::SUCCESS,
        Ok(_) => ExitCode::from(1),
        Err(e) => {
            eprintln!("shadowdiff: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run(a: Args) -> Result<usize, String> {
    let client = redis::Client::open(a.redis.as_str()).map_err(|e| format!("redis url: {e}"))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("redis connect: {e}"))?;
    let cfg = ae_core::Config::from_map(&HashMap::from([(
        "AC_DATABASE_URL".to_owned(),
        a.pg.clone(),
    )]))
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
                    continue;
                }
            };
            if cutoff.is_some_and(|c| rec.recorded_at < c) {
                continue;
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
            sum.add(&out);
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    println!("{}", sum.line());
    Ok(sum.diff)
}
