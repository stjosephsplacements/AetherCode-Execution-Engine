use crate::events::push_event;
use crate::stream::STREAM_MAXLEN;
use crate::{Error, Queue, Result};
use ae_core::eventlog::{Event, EventType};
use ae_core::model::{Checker, Job, Limits};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rand::RngCore;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

/// Lifetime of the `ac:tests:<job>` hash (Go: 1h).
const TEST_DATA_TTL: Duration = Duration::from_secs(60 * 60);
/// Marks an AES-256-GCM ciphertext; NUL is never valid base64, so plaintext cannot collide.
const ENC_SENTINEL: &[u8] = b"enc:\x00";
const MAX_TEST_COUNT: i64 = 10_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TestData {
    pub input: String,
    pub expected_output: String,
    /// Optional `KEY=VALUE` entries for this test's run.
    pub env: Vec<String>,
}

/// Everything a job carries besides its source, limits and checker (Go's `TestSet`).
/// Empty strings mean "absent", as in Go.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TestSet {
    pub tests: Vec<TestData>,
    pub checker_code: String,
    pub checker_language: String,
    pub interactor_code: String,
    pub interactor_language: String,
    pub files: BTreeMap<String, String>,
    pub compiler_flags: Vec<String>,
    pub callback_url: String,
    /// Plaintext; stored encrypted when `AC_WEBHOOK_MASTER_KEY` is set.
    pub callback_secret: String,
    /// `full` (empty), `preview` or `none`.
    pub output_mode: String,
    /// W3C `traceparent` of the request that queued the job (empty when none); the
    /// worker parents its job span on it. Go ignores the extra hash field.
    pub traceparent: String,
}

/// What [`Queue::get_tests`] reads back from `ac:tests:<job>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSpec {
    pub limits: Limits,
    pub checker: Checker,
    pub set: TestSet,
}

fn encrypt(key: &[u8], plaintext: &str) -> Result<Vec<u8>> {
    let gcm = Aes256Gcm::new_from_slice(key).map_err(|e| Error::Crypto(e.to_string()))?;
    let mut nonce = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ct = gcm
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
        .map_err(|e| Error::Crypto(e.to_string()))?;
    let mut raw = nonce.to_vec();
    raw.extend_from_slice(&ct);
    let mut out = ENC_SENTINEL.to_vec();
    out.extend_from_slice(STANDARD.encode(raw).as_bytes());
    Ok(out)
}

/// Reverses [`encrypt`]; a value without the sentinel is legacy plaintext and passes through.
fn decrypt(key: Option<&[u8]>, stored: &[u8]) -> Result<String> {
    let Some(b64) = stored.strip_prefix(ENC_SENTINEL) else {
        return String::from_utf8(stored.to_vec())
            .map_err(|_| Error::Corrupt("callback_secret is not UTF-8".into()));
    };
    let key = key.ok_or_else(|| {
        Error::Crypto(
            "callback_secret is encrypted but AC_WEBHOOK_MASTER_KEY is not configured".into(),
        )
    })?;
    let raw = STANDARD
        .decode(b64)
        .map_err(|e| Error::Crypto(format!("base64 decode secret: {e}")))?;
    if raw.len() < 12 {
        return Err(Error::Crypto("ciphertext too short".into()));
    }
    let gcm = Aes256Gcm::new_from_slice(key).map_err(|e| Error::Crypto(e.to_string()))?;
    let pt = gcm
        .decrypt(Nonce::from_slice(&raw[..12]), &raw[12..])
        .map_err(|_| Error::Crypto("gcm decrypt failed".into()))?;
    String::from_utf8(pt).map_err(|_| Error::Crypto("secret is not UTF-8".into()))
}

// Caller-supplied env and argument limits (Go: internal/sandbox/env.go, args.go).
const MAX_ENV_VARS: usize = 32;
const MAX_ENV_VALUE_BYTES: usize = 4 << 10;
const MAX_ENV_TOTAL_BYTES: usize = 8 << 10;
const MAX_ARGS: usize = 16;
const MAX_ARG_BYTES: usize = 1 << 10;
const MAX_ARGS_TOTAL: usize = 4 << 10;

const DENIED_ENV_PREFIXES: &[&str] = &[
    "LD_", "DYLD_", "MALLOC_", "PYTHON", "NODE_", "BUN_", "JAVA_", "_JAVA_", "JDK_", "MONO_",
    "RUBY", "PHP", "BASH",
];
/// Go's `deniedEnvKeys` plus `reservedEnvKeys` (every key the language table sets).
const DENIED_ENV_KEYS: &[&str] = &[
    "GLIBC_TUNABLES",
    "GCONV_PATH",
    "LOCPATH",
    "NLSPATH",
    "TMPDIR",
    "IFS",
    "ENV",
    "SHELLOPTS",
    "PS4",
    "GODEBUG",
    "GOGC",
    "GOMEMLIMIT",
    "GOMAXPROCS",
    "GOTRACEBACK",
    "GOCACHE",
    "GOROOT",
    "HOME",
    "JAVA_HOME",
    "KOTLIN_HOME",
    "PATH",
];

fn valid_env_key(k: &str) -> bool {
    let mut b = k.bytes();
    b.next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
        && b.all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

/// Port of Go's `sandbox.ValidateEnv`.
pub(crate) fn validate_env(env: &[String]) -> std::result::Result<(), String> {
    if env.len() > MAX_ENV_VARS {
        return Err(format!(
            "too many variables (maximum {MAX_ENV_VARS} allowed)"
        ));
    }
    let mut seen = HashSet::new();
    let mut total = 0;
    for kv in env {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("entry {kv:?} must be KEY=VALUE"))?;
        if !valid_env_key(k) {
            return Err(format!("key {k:?} is invalid"));
        }
        if v.contains('\0') {
            return Err(format!("value for {k:?} contains a NUL byte"));
        }
        if v.len() > MAX_ENV_VALUE_BYTES {
            return Err(format!(
                "value for {k:?} exceeds {MAX_ENV_VALUE_BYTES} bytes"
            ));
        }
        if DENIED_ENV_KEYS.contains(&k) || DENIED_ENV_PREFIXES.iter().any(|p| k.starts_with(p)) {
            return Err(format!("key {k:?} is not permitted"));
        }
        if !seen.insert(k) {
            return Err(format!("key {k:?} is duplicated"));
        }
        total += kv.len();
        if total > MAX_ENV_TOTAL_BYTES {
            return Err(format!("total size exceeds {MAX_ENV_TOTAL_BYTES} bytes"));
        }
    }
    Ok(())
}

/// Port of Go's `sandbox.ValidateArgs`.
pub(crate) fn validate_args(args: &[String]) -> std::result::Result<(), String> {
    if args.len() > MAX_ARGS {
        return Err(format!("at most {MAX_ARGS} arguments"));
    }
    let mut total = 0;
    for (i, a) in args.iter().enumerate() {
        if a.len() > MAX_ARG_BYTES {
            return Err(format!("argument {i} is longer than {MAX_ARG_BYTES} bytes"));
        }
        if a.contains('\0') {
            return Err(format!("argument {i} contains a NUL byte"));
        }
        total += a.len();
    }
    if total > MAX_ARGS_TOTAL {
        return Err(format!("arguments total more than {MAX_ARGS_TOTAL} bytes"));
    }
    Ok(())
}

fn json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| Error::Corrupt(e.to_string()))
}

fn checker_name(c: Checker) -> &'static str {
    match c {
        Checker::Exact => "exact",
        Checker::Token => "token",
        Checker::Float => "float",
    }
}

impl Queue {
    /// Go's `PrepareAndEnqueue`, in one pipeline: stores the test data in `ac:tests:<job>`
    /// (skipped when there are no tests; limits and checker come from `job`), logs and
    /// publishes the QUEUED event, and `XADD`s `submission_id` plus `expires_at` (unix ms,
    /// when `job.expires_at` is set) to `stream`.
    ///
    /// # Errors
    /// Fails on encryption, serialisation or Redis errors.
    pub async fn prepare_and_enqueue(&self, job: &Job, set: &TestSet, stream: &str) -> Result<()> {
        let id = &job.job_id;
        let mut pipe = redis::pipe();
        if !set.tests.is_empty() {
            let secret: Vec<u8> = match (&self.master_key, set.callback_secret.is_empty()) {
                (Some(k), false) => encrypt(k, &set.callback_secret)?,
                _ => set.callback_secret.clone().into_bytes(),
            };
            let l = &job.limits;
            let mut f: Vec<(String, Vec<u8>)> = vec![
                ("count".into(), set.tests.len().to_string().into_bytes()),
                ("cpu_ns".into(), l.cpu_ns.to_string().into_bytes()),
                ("mem_bytes".into(), l.mem_bytes.to_string().into_bytes()),
                ("stack_bytes".into(), l.stack_bytes.to_string().into_bytes()),
                ("procs".into(), l.procs.to_string().into_bytes()),
                ("checker".into(), checker_name(job.checker).into()),
                ("checker_code".into(), set.checker_code.clone().into_bytes()),
                (
                    "checker_language".into(),
                    set.checker_language.clone().into_bytes(),
                ),
                (
                    "interactor_code".into(),
                    set.interactor_code.clone().into_bytes(),
                ),
                (
                    "interactor_language".into(),
                    set.interactor_language.clone().into_bytes(),
                ),
                ("callback_url".into(), set.callback_url.clone().into_bytes()),
                ("callback_secret".into(), secret),
                ("output_mode".into(), set.output_mode.clone().into_bytes()),
            ];
            if !set.traceparent.is_empty() {
                f.push(("traceparent".into(), set.traceparent.clone().into_bytes()));
            }
            if !set.files.is_empty() {
                f.push(("files_json".into(), json(&set.files)?.into_bytes()));
            }
            if !l.args.is_empty() {
                f.push(("args_json".into(), json(&l.args)?.into_bytes()));
            }
            if !set.compiler_flags.is_empty() {
                f.push((
                    "compiler_flags_json".into(),
                    json(&set.compiler_flags)?.into_bytes(),
                ));
            }
            for (i, t) in set.tests.iter().enumerate() {
                f.push((format!("{i}:input"), t.input.clone().into_bytes()));
                f.push((
                    format!("{i}:expected"),
                    t.expected_output.clone().into_bytes(),
                ));
                if !t.env.is_empty() {
                    f.push((format!("{i}:env_json"), json(&t.env)?.into_bytes()));
                }
            }
            let key = format!("ac:tests:{id}");
            pipe.cmd("HSET").arg(&key).arg(&f).ignore();
            pipe.cmd("EXPIRE")
                .arg(&key)
                .arg(TEST_DATA_TTL.as_secs())
                .ignore();
        }
        push_event(&mut pipe, &Event::new(id, EventType::Queued, None));
        let xadd = pipe
            .cmd("XADD")
            .arg(stream)
            .arg("MAXLEN")
            .arg("~")
            .arg(STREAM_MAXLEN)
            .arg("*")
            .arg("submission_id")
            .arg(id);
        if let Some(exp) = job.expires_at {
            xadd.arg("expires_at").arg(exp);
        }
        xadd.ignore();
        pipe.query_async::<()>(&mut self.conn.clone()).await?;
        Ok(())
    }

    /// Go's `GetTests`: reads `ac:tests:<job>` in one round trip. Fields absent on hashes
    /// written by older engines keep their defaults; corrupt env or args fail the job.
    ///
    /// # Errors
    /// Fails when the hash is missing or corrupt, or the secret cannot be decrypted.
    pub async fn get_tests(&self, job_id: &str) -> Result<JobSpec> {
        let key = format!("ac:tests:{job_id}");
        let raw: HashMap<String, Vec<u8>> = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(&mut self.conn.clone())
            .await?;
        let s = |k: &str| {
            raw.get(k)
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default()
        };
        let corrupt = |m: String| Error::Corrupt(format!("{m} in {key}"));

        let count_s = raw
            .get("count")
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .ok_or_else(|| corrupt("no test count".into()))?;
        let count: i64 = count_s
            .parse()
            .map_err(|_| corrupt(format!("corrupt test count {count_s:?}")))?;
        if !(0..=MAX_TEST_COUNT).contains(&count) {
            return Err(corrupt(format!("invalid test count {count}")));
        }

        let mut tests = Vec::new();
        for i in 0..count {
            let mut t = TestData {
                input: s(&format!("{i}:input")),
                expected_output: s(&format!("{i}:expected")),
                env: Vec::new(),
            };
            // The writer never stores an empty list, so a present field must decode to a
            // non-empty valid env; anything else fails rather than running without it.
            if let Some(ej) = raw.get(&format!("{i}:env_json")) {
                let env: Vec<String> = serde_json::from_slice::<Option<Vec<String>>>(ej)
                    .map_err(|e| e.to_string())
                    .and_then(|v| {
                        v.filter(|v| !v.is_empty())
                            .ok_or_else(|| "empty env".into())
                    })
                    .and_then(|v| validate_env(&v).map(|()| v))
                    .map_err(|e| corrupt(format!("corrupt env for test {i}: {e}")))?;
                t.env = env;
            }
            tests.push(t);
        }

        let num = |k: &str| s(k).parse::<u64>().unwrap_or(0);
        let mut limits = Limits {
            cpu_ns: num("cpu_ns"),
            mem_bytes: num("mem_bytes"),
            stack_bytes: num("stack_bytes"),
            procs: num("procs"),
            args: Vec::new(),
        };
        if let Some(aj) = raw.get("args_json") {
            let args: Vec<String> = serde_json::from_slice::<Option<Vec<String>>>(aj)
                .map_err(|e| e.to_string())
                .map(Option::unwrap_or_default)
                .and_then(|v| validate_args(&v).map(|()| v))
                .map_err(|e| corrupt(format!("corrupt args: {e}")))?;
            limits.args = args;
        }
        // Go ignores undecodable files and compiler flags.
        let files = raw
            .get("files_json")
            .and_then(|v| serde_json::from_slice(v).ok())
            .unwrap_or_default();
        let compiler_flags = raw
            .get("compiler_flags_json")
            .and_then(|v| serde_json::from_slice(v).ok())
            .unwrap_or_default();
        let callback_secret = match raw.get("callback_secret") {
            Some(v) if !v.is_empty() => decrypt(self.master_key.as_deref(), v)?,
            _ => String::new(),
        };

        Ok(JobSpec {
            limits,
            checker: Checker::from_name(&s("checker")),
            set: TestSet {
                tests,
                checker_code: s("checker_code"),
                checker_language: s("checker_language"),
                interactor_code: s("interactor_code"),
                interactor_language: s("interactor_language"),
                files,
                compiler_flags,
                callback_url: s("callback_url"),
                callback_secret,
                output_mode: s("output_mode"),
                traceparent: s("traceparent"),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_round_trips_and_carries_sentinel() {
        let k = [7u8; 32];
        let ct = encrypt(&k, "hello").unwrap();
        assert!(ct.starts_with(ENC_SENTINEL));
        assert_eq!(decrypt(Some(&k), &ct).unwrap(), "hello");
        assert!(decrypt(Some(&[8u8; 32]), &ct).is_err());
        assert!(decrypt(None, &ct).is_err());
        assert_eq!(decrypt(None, b"plain").unwrap(), "plain");
    }

    #[test]
    fn env_validation_matches_go() {
        let ok = |v: &[&str]| validate_env(&v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>());
        assert!(ok(&["A=1", "_b9=x="]).is_ok());
        for bad in [
            &["A"][..],
            &["1A=x"],
            &["A=\0"],
            &["LD_PRELOAD=x"],
            &["PATH=x"],
            &["HOME=x"],
            &["A=1", "A=2"],
        ] {
            assert!(ok(bad).is_err(), "{bad:?}");
        }
        let long = format!("A={}", "x".repeat(MAX_ENV_VALUE_BYTES + 1));
        assert!(ok(&[long.as_str()]).is_err());
    }

    #[test]
    fn args_validation_matches_go() {
        let v = |n: usize, len: usize| vec!["a".repeat(len); n];
        assert!(validate_args(&v(16, 256)).is_ok());
        assert!(validate_args(&v(17, 1)).is_err());
        assert!(validate_args(&v(1, MAX_ARG_BYTES + 1)).is_err());
        assert!(validate_args(&v(5, 1000)).is_err());
        assert!(validate_args(&["a\0".to_owned()]).is_err());
    }
}
