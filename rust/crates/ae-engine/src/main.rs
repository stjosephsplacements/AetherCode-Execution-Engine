//! `engine`: the AetherCode execution engine binary (Go: `cmd/stj-exec`).
//!
//! Startup: config, telemetry, Postgres (connect + advisory-locked migrate), Redis,
//! stale-entry purge, sandbox health and language self-tests, pre-warm, then ready.
//! Shutdown on SIGTERM/SIGINT: readiness off, stop accepting connections, stop
//! consuming, wait up to 120 s for in-flight jobs, close HTTP, flush telemetry, exit 0.
//! A second signal exits immediately with status 1.

mod shadow;
mod startup;
mod supervisor;
mod telemetry;

use ae_core::Config;
use clap::{Parser, ValueEnum};
use std::process::ExitCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Role {
    All,
    Gateway,
    Worker,
}

impl Role {
    fn parse(s: &str) -> Option<Self> {
        <Self as ValueEnum>::from_str(s, true).ok()
    }
    pub fn gateway(self) -> bool {
        matches!(self, Self::All | Self::Gateway)
    }
    pub fn worker(self) -> bool {
        matches!(self, Self::All | Self::Worker)
    }
}

#[derive(Debug, Parser)]
#[command(name = "engine", about = "AetherCode execution engine")]
struct Cli {
    /// Components to run; defaults to AC_ROLE (all).
    #[arg(long, value_enum)]
    role: Option<Role>,
    /// Worker concurrency budget; defaults to AC_WORKERS (0 = a quarter of the cores).
    #[arg(long)]
    workers: Option<usize>,
    /// Compare-only worker beside the Go engine (requires --role worker).
    #[arg(long)]
    shadow: bool,
}

/// Everything decided before the runtime starts.
pub struct Settings {
    pub cfg: Config,
    pub role: Role,
    pub shadow: bool,
}

fn settings(cli: Cli, mut cfg: Config) -> Result<Settings, String> {
    let role = match cli.role {
        Some(r) => r,
        None => Role::parse(&cfg.role)
            .ok_or_else(|| format!("AC_ROLE must be all, gateway or worker, got {:?}", cfg.role))?,
    };
    if let Some(w) = cli.workers {
        cfg.workers = w;
    }
    if cli.shadow && role != Role::Worker {
        return Err(
            "--shadow runs a compare-only worker; it requires --role worker (the gateway cannot run in shadow mode)"
                .to_owned(),
        );
    }
    if telemetry::Format::parse(&cfg.log_format).is_none() {
        return Err(format!(
            "AC_LOG_FORMAT must be json, text, gcp or cloudwatch, got {:?}",
            cfg.log_format
        ));
    }
    Ok(Settings {
        cfg,
        role,
        shadow: cli.shadow,
    })
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let s = match Config::from_env()
        .map_err(|e| e.to_string())
        .and_then(|cfg| settings(cli, cfg))
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("fatal: {e}");
            return ExitCode::FAILURE;
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("fatal: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async {
        let format = telemetry::Format::parse(&s.cfg.log_format).unwrap_or(telemetry::Format::Json);
        let tel = match telemetry::init(
            format,
            telemetry::level(&s.cfg.log_level),
            &s.cfg.otlp_endpoint,
        ) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("fatal: {e}");
                return ExitCode::FAILURE;
            }
        };
        let code = match startup::run(s).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "fatal");
                ExitCode::FAILURE
            }
        };
        tel.shutdown();
        code
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Config {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        Config::from_map(&m).unwrap()
    }

    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("engine").chain(args.iter().copied()))
    }

    #[test]
    fn flags_override_env() {
        let s = settings(
            cli(&["--role", "worker", "--workers", "3"]),
            cfg(&[("AC_ROLE", "gateway")]),
        )
        .unwrap();
        assert_eq!(s.role, Role::Worker);
        assert_eq!(s.cfg.workers, 3);
        let s = settings(
            cli(&[]),
            cfg(&[("AC_ROLE", "gateway"), ("AC_WORKERS", "5")]),
        )
        .unwrap();
        assert_eq!(s.role, Role::Gateway);
        assert_eq!(s.cfg.workers, 5);
        assert!(settings(cli(&[]), cfg(&[("AC_ROLE", "both")])).is_err());
    }

    #[test]
    fn shadow_requires_worker_role() {
        assert!(settings(cli(&["--shadow"]), cfg(&[])).is_err());
        assert!(settings(cli(&["--shadow", "--role", "gateway"]), cfg(&[])).is_err());
        assert!(
            settings(cli(&["--shadow", "--role", "worker"]), cfg(&[]))
                .unwrap()
                .shadow
        );
    }

    #[test]
    fn rejects_unknown_log_format() {
        assert!(settings(cli(&[]), cfg(&[("AC_LOG_FORMAT", "xml")])).is_err());
    }
}
