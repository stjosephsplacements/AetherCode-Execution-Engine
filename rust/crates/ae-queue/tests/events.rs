mod common;

use ae_core::eventlog::{Event, EventType};
use ae_core::model::{Checker, Job, Limits};
use ae_queue::{TestData, TestSet};
use common::{env, env_with};
use redis::AsyncCommands;
use serde_json::json;
use std::collections::BTreeMap;

const KEY_HEX: &str = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";

fn job(id: &str) -> Job {
    Job {
        job_id: id.into(),
        submission_id: id.into(),
        language: "cpp".into(),
        source: String::new(),
        limits: Limits {
            cpu_ns: 2_000_000_000,
            mem_bytes: 256 << 20,
            stack_bytes: 0,
            procs: 0,
            args: vec![],
        },
        checker: Checker::Exact,
        tests_key: format!("ac:tests:{id}"),
        expires_at: None,
    }
}

fn td(i: &str, o: &str) -> TestData {
    TestData {
        input: i.into(),
        expected_output: o.into(),
        env: vec![],
    }
}

fn set2() -> TestSet {
    TestSet {
        tests: vec![td("1 2", "3"), td("4 5", "9")],
        ..TestSet::default()
    }
}

#[tokio::test]
async fn prepare_and_enqueue_atomic_pipeline() {
    let e = env().await;
    e.q.prepare_and_enqueue(&job("job-abc"), &set2(), "ac:test:stream")
        .await
        .unwrap();
    let got = e.q.get_tests("job-abc").await.unwrap();
    assert_eq!(got.set.tests, set2().tests);
    assert_eq!(got.limits.cpu_ns, 2_000_000_000);
    assert_eq!(got.limits.mem_bytes, 256 << 20);
    assert_eq!(e.q.depth("ac:test:stream").await.unwrap(), 1);
    let log = e.q.replay("job-abc").await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].kind, EventType::Queued);
}

#[tokio::test]
async fn test_data_and_event_log_have_go_ttls() {
    let mut e = env().await;
    e.q.prepare_and_enqueue(&job("ttl"), &set2(), "ac:s")
        .await
        .unwrap();
    let t: i64 = e.rdb.ttl("ac:tests:ttl").await.unwrap();
    assert!((3590..=3600).contains(&t), "{t}");
    let t: i64 = e.rdb.ttl("ac:events:ttl:log").await.unwrap();
    assert!((590..=600).contains(&t), "{t}");
}

#[tokio::test]
async fn get_tests_round_trips_every_field() {
    let e = env().await;
    let mut j = job("job-all");
    j.checker = Checker::Float;
    j.limits = Limits {
        cpu_ns: 1,
        mem_bytes: 2,
        stack_bytes: 64 << 20,
        procs: 4,
        args: vec!["--fast".into(), "x y".into()],
    };
    let set = TestSet {
        tests: vec![
            TestData {
                input: "a".into(),
                expected_output: "b".into(),
                env: vec!["MODE=fast".into(), "N=3".into()],
            },
            td("c", "d"),
        ],
        checker_code: "int main(){}".into(),
        checker_language: "cpp".into(),
        interactor_code: "print(1)".into(),
        interactor_language: "python".into(),
        files: BTreeMap::from([("lib.h".to_owned(), "#pragma once".to_owned())]),
        compiler_flags: vec!["-O2".into()],
        callback_url: "https://hook.example/cb".into(),
        callback_secret: "s3cret".into(),
        output_mode: "preview".into(),
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
    };
    e.q.prepare_and_enqueue(&j, &set, "ac:s").await.unwrap();
    let got = e.q.get_tests("job-all").await.unwrap();
    assert_eq!(got.set, set);
    assert_eq!(got.limits, j.limits);
    assert_eq!(got.checker, Checker::Float);
}

#[tokio::test]
async fn zero_limits_preserve_defaults_and_old_hashes_read() {
    let mut e = env().await;
    let _: () = e
        .rdb
        .hset_multiple(
            "ac:tests:job-old",
            &[("count", "1"), ("0:input", "a"), ("0:expected", "b")],
        )
        .await
        .unwrap();
    let got = e.q.get_tests("job-old").await.unwrap();
    assert_eq!(got.limits, Limits::default());
    assert_eq!(got.checker, Checker::Exact);
    assert_eq!(got.set.output_mode, "");
    assert_eq!(got.set.tests, [td("a", "b")]);
}

#[tokio::test]
async fn missing_or_corrupt_count_is_an_error() {
    let mut e = env().await;
    assert!(e.q.get_tests("nope").await.is_err());
    for c in ["x", "-1", "10001"] {
        let _: () = e.rdb.hset("ac:tests:bad", "count", c).await.unwrap();
        assert!(e.q.get_tests("bad").await.is_err(), "count {c}");
    }
}

#[tokio::test]
async fn corrupt_env_fails_job() {
    let mut e = env().await;
    for v in [
        "{not json",
        "",
        "null",
        "[]",
        r#"["LD_PRELOAD=/w/x.so"]"#,
        r#"["PATH=/x"]"#,
        r#"["A=1","A=2"]"#,
    ] {
        let _: () = e
            .rdb
            .hset_multiple(
                "ac:tests:job-bad",
                &[
                    ("count", "1"),
                    ("0:input", "a"),
                    ("0:expected", "b"),
                    ("0:env_json", v),
                ],
            )
            .await
            .unwrap();
        assert!(e.q.get_tests("job-bad").await.is_err(), "env_json {v:?}");
    }
}

#[tokio::test]
async fn corrupt_args_fail_job() {
    let mut e = env().await;
    for v in ["nope", r#"["a\u0000b"]"#] {
        let _: () = e
            .rdb
            .hset_multiple(
                "ac:tests:job-args",
                &[
                    ("count", "1"),
                    ("0:input", ""),
                    ("0:expected", ""),
                    ("args_json", v),
                ],
            )
            .await
            .unwrap();
        assert!(e.q.get_tests("job-args").await.is_err(), "args_json {v:?}");
    }
}

#[tokio::test]
async fn no_tests_skips_hash_but_enqueues_and_logs() {
    let mut e = env().await;
    e.q.prepare_and_enqueue(&job("empty"), &TestSet::default(), "ac:s")
        .await
        .unwrap();
    let exists: bool = e.rdb.exists("ac:tests:empty").await.unwrap();
    assert!(!exists);
    assert_eq!(e.q.depth("ac:s").await.unwrap(), 1);
    assert_eq!(e.q.replay("empty").await.unwrap().len(), 1);
}

#[tokio::test]
async fn multiple_enqueues_use_independent_streams() {
    let e = env().await;
    e.q.prepare_and_enqueue(&job("a"), &set2(), "ac:one")
        .await
        .unwrap();
    e.q.prepare_and_enqueue(&job("b"), &set2(), "ac:two")
        .await
        .unwrap();
    e.q.prepare_and_enqueue(&job("c"), &set2(), "ac:two")
        .await
        .unwrap();
    assert_eq!(e.q.depth("ac:one").await.unwrap(), 1);
    assert_eq!(e.q.depth("ac:two").await.unwrap(), 2);
}

#[tokio::test]
async fn callback_secret_is_encrypted_with_master_key() {
    let mut e = env_with(&[("AC_WEBHOOK_MASTER_KEY", KEY_HEX)]).await;
    let set = TestSet {
        callback_url: "https://x/cb".into(),
        callback_secret: "whsec_abc".into(),
        ..set2()
    };
    e.q.prepare_and_enqueue(&job("job-enc"), &set, "ac:s")
        .await
        .unwrap();
    let raw: Vec<u8> = e
        .rdb
        .hget("ac:tests:job-enc", "callback_secret")
        .await
        .unwrap();
    assert!(raw.starts_with(b"enc:\0"), "{raw:?}");
    assert!(!String::from_utf8_lossy(&raw).contains("whsec_abc"));
    let got = e.q.get_tests("job-enc").await.unwrap();
    assert_eq!(got.set.callback_secret, "whsec_abc");
    assert_eq!(got.set.callback_url, "https://x/cb");
}

#[tokio::test]
async fn go_encrypted_secret_decrypts() {
    // Produced by Go's encryptSecret with key bytes 1..=32 (KEY_HEX).
    const GO_CT: &[u8] = b"enc:\x00ZX5+zO5DPeWLf0c/Q0M9pMz0v7h71Qtq/aMmNDEPvwQhHlRX/RbrpbWGmzk=";
    let mut e = env_with(&[("AC_WEBHOOK_MASTER_KEY", KEY_HEX)]).await;
    let _: () = e
        .rdb
        .hset_multiple(
            "ac:tests:go",
            &[
                ("count", &b"1"[..]),
                ("0:input", b""),
                ("0:expected", b""),
                ("callback_secret", GO_CT),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        e.q.get_tests("go").await.unwrap().set.callback_secret,
        "whsec_go_fixture"
    );
}

#[tokio::test]
async fn legacy_plaintext_secret_passes_through_with_key() {
    let mut e = env_with(&[("AC_WEBHOOK_MASTER_KEY", KEY_HEX)]).await;
    let _: () = e
        .rdb
        .hset_multiple(
            "ac:tests:legacy",
            &[
                ("count", "1"),
                ("0:input", ""),
                ("0:expected", ""),
                ("callback_secret", "plain"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        e.q.get_tests("legacy").await.unwrap().set.callback_secret,
        "plain"
    );
}

#[tokio::test]
async fn no_key_stores_plaintext_and_encrypted_without_key_errors() {
    let mut e = env().await;
    let set = TestSet {
        callback_secret: "plain-secret".into(),
        ..set2()
    };
    e.q.prepare_and_enqueue(&job("nokey"), &set, "ac:s")
        .await
        .unwrap();
    let raw: String = e
        .rdb
        .hget("ac:tests:nokey", "callback_secret")
        .await
        .unwrap();
    assert_eq!(raw, "plain-secret");
    let _: () = e
        .rdb
        .hset("ac:tests:nokey", "callback_secret", "enc:\0AAAA")
        .await
        .unwrap();
    assert!(e.q.get_tests("nokey").await.is_err());
}

#[tokio::test]
async fn publish_and_replay_in_order() {
    let e = env().await;
    assert!(e.q.replay("none").await.unwrap().is_empty());
    let a = Event::new("j1", EventType::Compiling, None);
    let b = Event::new("j1", EventType::TestResult, Some(json!({"test_index": 0})));
    let c = Event::new(
        "j1",
        EventType::Verdict,
        Some(json!({"verdict": "accepted"})),
    );
    for ev in [&a, &b, &c] {
        e.q.publish(ev).await.unwrap();
    }
    assert_eq!(e.q.replay("j1").await.unwrap(), [a, b, c]);
}

#[tokio::test]
async fn event_log_keeps_last_500_and_is_go_json() {
    let mut e = env().await;
    for i in 0..505 {
        e.q.publish(&Event::new("big", EventType::TestResult, Some(json!(i))))
            .await
            .unwrap();
    }
    let log = e.q.replay("big").await.unwrap();
    assert_eq!(log.len(), 500);
    assert_eq!(log[0].data, Some(json!(5)));
    let ev = Event::new("raw", EventType::Running, Some(json!("<x>")));
    e.q.publish(&ev).await.unwrap();
    let raw: Vec<Vec<u8>> = e.rdb.lrange("ac:events:raw:log", 0, -1).await.unwrap();
    assert_eq!(raw, [ev.to_json()]);
}

#[tokio::test]
async fn stream_token_is_stored_per_job_with_ttl() {
    let mut e = env().await;
    let tok = e.q.issue_stream_token("job-t").await.unwrap();
    assert_eq!(tok.len(), 27); // 20 random bytes, unpadded base64url
    assert!(
        tok.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    );
    let stored: String = e.rdb.get("ac:stoken:job-t").await.unwrap();
    assert_eq!(stored, tok);
    let ttl: i64 = e.rdb.ttl("ac:stoken:job-t").await.unwrap();
    assert!((1790..=1800).contains(&ttl), "{ttl}");
    assert!(e.q.verify_stream_token("job-t", &tok).await);
    assert!(!e.q.verify_stream_token("job-t", "wrong").await);
    assert!(!e.q.verify_stream_token("job-t", "").await);
    assert!(!e.q.verify_stream_token("other", &tok).await);
    assert_ne!(e.q.issue_stream_token("job-t").await.unwrap(), tok);
}
