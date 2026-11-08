use crate::error::ConfigError;
use std::collections::HashMap;
use std::time::Duration;

/// Engine configuration, one field per `AC_*` variable. Defaults match the Go engine
/// except where noted on the field.
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub gateway_addr: String,
    pub database_url: String,
    pub redis_addr: String,
    pub redis_password: String,
    pub judge_url: String,
    pub judge_token: String,
    pub worker_count: usize,
    pub db_max_conns: usize,
    pub redis_pool: usize,
    pub redis_min_idle: usize,
    pub stream_name: String,
    pub consumer_group: String,
    pub log_level: String,
    pub log_format: String,

    pub auth_enabled: bool,
    pub allow_unauth: bool,
    pub oidc_issuer: String,
    pub oidc_jwks_url: String,
    pub oidc_audience: String,
    pub database_app_url: String,

    pub rate_limit_per_minute: u32,
    pub rate_limit_burst: u32,
    pub rate_limit_unauth_per_minute: u32,
    pub rate_limit_unauth_burst: u32,
    /// 0 means `effective_workers() * 4`, see [`Config::max_queue_depth`].
    pub admission_max_queue_depth: usize,

    pub cors_allowed_origins: String,
    pub trusted_proxies: Vec<String>,

    pub submit_stream_name: String,
    pub run_stream_name: String,
    pub bulk_stream_name: String,
    pub submit_weight: u32,
    pub bulk_weight: u32,

    pub job_timeout: Duration,

    pub test_parallelism: usize,
    /// Go default is 60s; the Rust engine defaults to 5m.
    pub compile_cache_ttl: Duration,
    pub block_read_timeout: Duration,

    pub sandbox_stdout_max_bytes: u64,
    pub sandbox_stderr_max_bytes: u64,

    /// AES-256-GCM key (64 hex chars) for webhook secrets.
    pub webhook_master_key: Option<Vec<u8>>,

    /// `AC_WORKERS`: 0 means auto, see [`Config::effective_workers`].
    pub workers: usize,
    pub compile_cache_max_mb: u64,
    pub prewarm: usize,
    pub role: String,
    pub otlp_endpoint: String,
    /// `AC_LANGUAGES_FILE`: TOML language table replacing the built-in one; empty uses the built-in.
    pub languages_file: String,
    /// `AC_DEBUG_PORT`: port of the loopback-only metrics listener (always 127.0.0.1).
    pub debug_port: u16,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let vars: HashMap<String, String> = std::env::vars().collect();
        Self::from_map(&vars)
    }

    pub fn from_map(m: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let s = |k: &'static str, d: &str| get(m, k).map_or_else(|| d.to_owned(), str::to_owned);
        Ok(Self {
            gateway_addr: s("AC_GATEWAY_ADDR", "127.0.0.1:5100"),
            database_url: s(
                "AC_DATABASE_URL",
                "postgres://aethercode:aethercode@127.0.0.1:5433/aethercode_exec",
            ),
            redis_addr: s("AC_REDIS_ADDR", "127.0.0.1:6379"),
            redis_password: s("AC_REDIS_PASSWORD", ""),
            judge_url: s("AC_JUDGE_URL", "http://127.0.0.1:5050"),
            judge_token: s("AC_JUDGE_TOKEN", ""),
            worker_count: num(m, "AC_WORKER_COUNT", 8)?,
            db_max_conns: num(m, "AC_DB_MAX_CONNS", 128)?,
            redis_pool: num(m, "AC_REDIS_POOL_SIZE", 256)?,
            redis_min_idle: num(m, "AC_REDIS_MIN_IDLE_CONNS", 32)?,
            stream_name: s("AC_STREAM_NAME", "ac:submissions"),
            consumer_group: s("AC_CONSUMER_GROUP", "workers"),
            log_level: s("AC_LOG_LEVEL", "info"),
            log_format: s("AC_LOG_FORMAT", "json"),

            auth_enabled: boolean(m, "AC_AUTH_ENABLED", true)?,
            allow_unauth: boolean(m, "AC_ALLOW_UNAUTH", false)?,
            oidc_issuer: s("AC_OIDC_ISSUER", "https://sso.example.com"),
            oidc_jwks_url: s("AC_OIDC_JWKS_URL", "http://127.0.0.1:8087/oauth/v2/keys"),
            oidc_audience: s("AC_OIDC_AUDIENCE", ""),
            database_app_url: s("AC_DATABASE_APP_URL", ""),

            rate_limit_per_minute: num(m, "AC_RATE_LIMIT_PER_MINUTE", 30)?,
            rate_limit_burst: num(m, "AC_RATE_LIMIT_BURST", 10)?,
            rate_limit_unauth_per_minute: num(m, "AC_RATE_LIMIT_UNAUTH_PER_MINUTE", 0)?,
            rate_limit_unauth_burst: num(m, "AC_RATE_LIMIT_UNAUTH_BURST", 5)?,
            admission_max_queue_depth: num(m, "AC_ADMISSION_MAX_QUEUE_DEPTH", 5000)?,

            cors_allowed_origins: s("AC_CORS_ALLOWED_ORIGINS", ""),
            trusted_proxies: get(m, "AC_TRUSTED_PROXIES")
                .map(|v| {
                    v.split(',')
                        .map(str::trim)
                        .filter(|p| !p.is_empty())
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),

            submit_stream_name: s("AC_SUBMIT_STREAM", "ac:submissions:submit"),
            run_stream_name: s("AC_RUN_STREAM", "ac:submissions:run"),
            bulk_stream_name: s("AC_BULK_STREAM", "ac:submissions:bulk"),
            submit_weight: num(m, "AC_SUBMIT_WEIGHT", 3)?,
            bulk_weight: num(m, "AC_BULK_WEIGHT", 1)?,

            job_timeout: duration(m, "AC_JOB_TIMEOUT", Duration::from_secs(90))?,

            test_parallelism: num(m, "AC_TEST_PARALLELISM", 4)?,
            compile_cache_ttl: duration(m, "AC_COMPILE_CACHE_TTL", Duration::from_secs(300))?,
            block_read_timeout: duration(m, "AC_BLOCK_READ_TIMEOUT", Duration::from_millis(500))?,

            sandbox_stdout_max_bytes: num(m, "AC_SANDBOX_STDOUT_MAX_BYTES", 8 << 20)?,
            sandbox_stderr_max_bytes: num(m, "AC_SANDBOX_STDERR_MAX_BYTES", 1 << 20)?,

            webhook_master_key: hex_key(m, "AC_WEBHOOK_MASTER_KEY", 32)?,

            workers: num(m, "AC_WORKERS", 0)?,
            compile_cache_max_mb: num(m, "AC_COMPILE_CACHE_MAX_MB", 1024)?,
            prewarm: num(m, "AC_PREWARM", 16)?,
            role: s("AC_ROLE", "all"),
            otlp_endpoint: s("AC_OTLP_ENDPOINT", ""),
            languages_file: s("AC_LANGUAGES_FILE", ""),
            debug_port: num(m, "AC_DEBUG_PORT", 6060)?,
        })
    }

    /// `AC_WORKERS`, or a quarter of the available cores (minimum 1) when 0.
    pub fn effective_workers(&self) -> usize {
        if self.workers > 0 {
            return self.workers;
        }
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        (cores / 4).max(1)
    }

    /// Admission queue depth limit, or `effective_workers() * 4` when configured as 0.
    pub fn max_queue_depth(&self) -> usize {
        if self.admission_max_queue_depth > 0 {
            self.admission_max_queue_depth
        } else {
            self.effective_workers() * 4
        }
    }
}

/// Empty values count as unset, like the Go loader.
fn get<'a>(m: &'a HashMap<String, String>, k: &str) -> Option<&'a str> {
    m.get(k).map(String::as_str).filter(|v| !v.is_empty())
}

fn invalid(key: &'static str, value: &str, expected: &'static str) -> ConfigError {
    ConfigError::Invalid {
        key,
        value: value.to_owned(),
        expected,
    }
}

fn num<T: std::str::FromStr>(
    m: &HashMap<String, String>,
    key: &'static str,
    default: T,
) -> Result<T, ConfigError> {
    get(m, key).map_or(Ok(default), |v| {
        v.parse().map_err(|_| invalid(key, v, "integer"))
    })
}

/// Accepts the same spellings as Go's `strconv.ParseBool`.
fn boolean(
    m: &HashMap<String, String>,
    key: &'static str,
    default: bool,
) -> Result<bool, ConfigError> {
    match get(m, key) {
        None => Ok(default),
        Some("1" | "t" | "T" | "TRUE" | "true" | "True") => Ok(true),
        Some("0" | "f" | "F" | "FALSE" | "false" | "False") => Ok(false),
        Some(v) => Err(invalid(key, v, "boolean")),
    }
}

fn duration(
    m: &HashMap<String, String>,
    key: &'static str,
    default: Duration,
) -> Result<Duration, ConfigError> {
    get(m, key).map_or(Ok(default), |v| {
        parse_duration(v).ok_or_else(|| invalid(key, v, "duration"))
    })
}

/// Parses Go-style durations such as `500ms`, `90s`, `1m30s`, `1.5h`.
fn parse_duration(s: &str) -> Option<Duration> {
    if s == "0" {
        return Some(Duration::ZERO);
    }
    let mut rest = s;
    let mut total = 0f64;
    while !rest.is_empty() {
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let n: f64 = rest[..end].parse().ok()?;
        rest = &rest[end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let secs = match &rest[..unit_end] {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        total += n * secs;
        rest = &rest[unit_end..];
    }
    Duration::try_from_secs_f64(total).ok()
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = "<redacted>";
        f.debug_struct("Config")
            .field("gateway_addr", &self.gateway_addr)
            .field("database_url", &r)
            .field("redis_addr", &self.redis_addr)
            .field("redis_password", &r)
            .field("judge_url", &self.judge_url)
            .field("judge_token", &r)
            .field(
                "webhook_master_key",
                &self.webhook_master_key.as_ref().map(|_| r),
            )
            .finish_non_exhaustive()
    }
}

fn hex_key(
    m: &HashMap<String, String>,
    key: &'static str,
    want: usize,
) -> Result<Option<Vec<u8>>, ConfigError> {
    let Some(v) = get(m, key) else {
        return Ok(None);
    };
    let bad = || invalid(key, "<redacted>", "hex string of 64 characters");
    if v.len() != want * 2 || !v.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad());
    }
    (0..v.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&v[i..i + 2], 16).map_err(|_| bad()))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect()
    }

    #[test]
    fn defaults_match_go() {
        let c = Config::from_map(&map(&[])).unwrap();
        assert_eq!(c.gateway_addr, "127.0.0.1:5100");
        assert_eq!(c.redis_addr, "127.0.0.1:6379");
        assert_eq!(c.judge_url, "http://127.0.0.1:5050");
        assert_eq!(c.worker_count, 8);
        assert_eq!(c.db_max_conns, 128);
        assert_eq!(c.redis_pool, 256);
        assert_eq!(c.redis_min_idle, 32);
        assert_eq!(c.stream_name, "ac:submissions");
        assert_eq!(c.consumer_group, "workers");
        assert!(c.auth_enabled);
        assert!(!c.allow_unauth);
        assert_eq!(c.rate_limit_per_minute, 30);
        assert_eq!(c.rate_limit_burst, 10);
        assert_eq!(c.rate_limit_unauth_per_minute, 0);
        assert_eq!(c.rate_limit_unauth_burst, 5);
        assert_eq!(c.admission_max_queue_depth, 5000);
        assert_eq!(c.submit_stream_name, "ac:submissions:submit");
        assert_eq!(c.run_stream_name, "ac:submissions:run");
        assert_eq!(c.bulk_stream_name, "ac:submissions:bulk");
        assert_eq!(c.submit_weight, 3);
        assert_eq!(c.bulk_weight, 1);
        assert_eq!(c.job_timeout, Duration::from_secs(90));
        assert_eq!(c.test_parallelism, 4);
        assert_eq!(c.compile_cache_ttl, Duration::from_secs(300));
        assert_eq!(c.block_read_timeout, Duration::from_millis(500));
        assert_eq!(c.sandbox_stdout_max_bytes, 8 << 20);
        assert_eq!(c.sandbox_stderr_max_bytes, 1 << 20);
        assert_eq!(c.workers, 0);
        assert_eq!(c.compile_cache_max_mb, 1024);
        assert_eq!(c.prewarm, 16);
        assert_eq!(c.role, "all");
        assert_eq!(c.otlp_endpoint, "");
        assert_eq!(c.languages_file, "");
        assert_eq!(c.debug_port, 6060);
        assert!(c.trusted_proxies.is_empty());
        assert!(c.webhook_master_key.is_none());
    }

    #[test]
    fn parses_overrides() {
        let c = Config::from_map(&map(&[
            ("AC_JOB_TIMEOUT", "1m30s"),
            ("AC_BLOCK_READ_TIMEOUT", "250ms"),
            ("AC_AUTH_ENABLED", "false"),
            ("AC_TRUSTED_PROXIES", "10.0.0.0/8, 192.168.0.0/16,"),
            ("AC_WEBHOOK_MASTER_KEY", &"ab".repeat(32)),
        ]))
        .unwrap();
        assert_eq!(c.job_timeout, Duration::from_secs(90));
        assert_eq!(c.block_read_timeout, Duration::from_millis(250));
        assert!(!c.auth_enabled);
        assert_eq!(c.trusted_proxies, ["10.0.0.0/8", "192.168.0.0/16"]);
        assert_eq!(c.webhook_master_key.unwrap(), vec![0xab; 32]);
    }

    #[test]
    fn rejects_bad_duration() {
        let e = Config::from_map(&map(&[("AC_JOB_TIMEOUT", "soon")])).unwrap_err();
        assert!(matches!(
            e,
            ConfigError::Invalid {
                key: "AC_JOB_TIMEOUT",
                ..
            }
        ));
    }

    #[test]
    fn rejects_bad_int_bool_and_key() {
        assert!(Config::from_map(&map(&[("AC_WORKER_COUNT", "x")])).is_err());
        assert!(Config::from_map(&map(&[("AC_ALLOW_UNAUTH", "maybe")])).is_err());
        assert!(Config::from_map(&map(&[("AC_WEBHOOK_MASTER_KEY", "abcd")])).is_err());
    }

    #[test]
    fn auto_workers_is_quarter_of_cores_min_one() {
        let c = Config::from_map(&map(&[])).unwrap();
        let cores = std::thread::available_parallelism().unwrap().get();
        assert_eq!(c.effective_workers(), (cores / 4).max(1));
        let c = Config::from_map(&map(&[("AC_WORKERS", "6")])).unwrap();
        assert_eq!(c.effective_workers(), 6);
    }

    #[test]
    fn auto_queue_depth_is_four_times_workers() {
        let c = Config::from_map(&map(&[
            ("AC_WORKERS", "3"),
            ("AC_ADMISSION_MAX_QUEUE_DEPTH", "0"),
        ]))
        .unwrap();
        assert_eq!(c.max_queue_depth(), 12);
        let c = Config::from_map(&map(&[("AC_WORKERS", "3")])).unwrap();
        assert_eq!(c.max_queue_depth(), 5000);
    }

    #[test]
    fn debug_port_parsed_and_validated() {
        let c = Config::from_map(&map(&[("AC_DEBUG_PORT", "7070")])).unwrap();
        assert_eq!(c.debug_port, 7070);
        assert!(Config::from_map(&map(&[("AC_DEBUG_PORT", "70000")])).is_err());
    }

    #[test]
    fn redis_password_read() {
        let c = Config::from_map(&map(&[("AC_REDIS_PASSWORD", "s3cret")])).unwrap();
        assert_eq!(c.redis_password, "s3cret");
    }

    #[test]
    fn hex_key_rejects_non_hex_characters() {
        let k = format!("+{}", "a".repeat(63));
        let err = Config::from_map(&map(&[("AC_WEBHOOK_MASTER_KEY", &k)]));
        assert!(err.is_err());
        let k = "a".repeat(64);
        assert!(Config::from_map(&map(&[("AC_WEBHOOK_MASTER_KEY", &k)])).is_ok());
    }

    #[test]
    fn debug_redacts_secrets() {
        let key = "ab".repeat(32);
        let c = Config::from_map(&map(&[
            ("AC_REDIS_PASSWORD", "hunter2"),
            ("AC_JUDGE_TOKEN", "tok-secret"),
            ("AC_DATABASE_URL", "postgres://u:dbpass@h/db"),
            ("AC_WEBHOOK_MASTER_KEY", &key),
        ]))
        .unwrap();
        let d = format!("{c:?}");
        for secret in ["hunter2", "tok-secret", "dbpass", "abab"] {
            assert!(!d.contains(secret), "{secret} leaked: {d}");
        }
    }
}
