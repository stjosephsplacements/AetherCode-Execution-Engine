use ae_loadgen::{Config, run, supported};
use clap::Parser;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "Student-simulating load generator for the AetherCode execution API")]
struct Args {
    /// Gateway base URL (Go or Rust engine).
    #[arg(long, default_value = "http://127.0.0.1:5100")]
    url: String,
    /// Bearer token, when the engine has auth enabled. Also read from AC_LOADGEN_TOKEN.
    #[arg(long, env = "AC_LOADGEN_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Concurrent simulated students.
    #[arg(long, default_value_t = 100)]
    students: usize,
    /// Comma-separated languages, picked uniformly.
    #[arg(long, value_delimiter = ',', default_value = "c,cpp,python,java")]
    languages: Vec<String>,
    /// How long students keep submitting.
    #[arg(long, default_value = "60s", value_parser = humantime::parse_duration)]
    duration: Duration,
    /// Spread student start times over this window.
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
    ramp: Duration,
    /// Minimum think time between a student's submissions.
    #[arg(long, default_value = "1s", value_parser = humantime::parse_duration)]
    think_min: Duration,
    /// Maximum think time between a student's submissions.
    #[arg(long, default_value = "3s", value_parser = humantime::parse_duration)]
    think_max: Duration,
    /// Per-job wait for the VERDICT.
    #[arg(long, default_value = "120s", value_parser = humantime::parse_duration)]
    timeout: Duration,
    /// Machine-readable JSON on stdout.
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let a = Args::parse();
    let known = supported();
    if let Some(bad) = a.languages.iter().find(|l| !known.contains(&l.as_str())) {
        eprintln!(
            "loadgen: unknown language {bad:?} (supported: {})",
            known.join(",")
        );
        return ExitCode::from(2);
    }
    if a.students == 0 {
        eprintln!("loadgen: --students must be at least 1");
        return ExitCode::from(2);
    }
    if !a.json {
        eprintln!(
            "loadgen: {} students, {}, {} for {:?}",
            a.students,
            a.url,
            a.languages.join(","),
            a.duration
        );
    }
    let cfg = Config {
        url: a.url.trim_end_matches('/').to_owned(),
        token: a.token.filter(|t| !t.is_empty()),
        students: a.students,
        languages: a.languages,
        duration: a.duration,
        ramp: a.ramp,
        think_min: a.think_min,
        think_max: a.think_max,
        timeout: a.timeout,
    };
    match run(cfg).await {
        Ok(r) => {
            if a.json {
                println!("{}", r.json());
            } else {
                print!("{}", r.table());
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("loadgen: {e}");
            ExitCode::from(2)
        }
    }
}
