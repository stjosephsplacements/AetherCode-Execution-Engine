use ae_core::Config;
use ae_core::model::{DEFAULT_TENANT_ID, Status, Submission, TestResult, Verdict};
use ae_db::{Db, connect, truncate};
use sqlx::Row;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

struct Env {
    _c: ContainerAsync<Postgres>,
    url: String,
    db: Db,
}

fn cfg(url: &str, app_url: &str) -> Config {
    let m = HashMap::from([
        ("AC_DATABASE_URL".to_owned(), url.to_owned()),
        ("AC_DATABASE_APP_URL".to_owned(), app_url.to_owned()),
    ]);
    Config::from_map(&m).unwrap()
}

async fn start() -> (ContainerAsync<Postgres>, String) {
    let c = Postgres::default()
        .with_user("aethercode")
        .with_password("aethercode")
        .with_db_name("aethercode_test")
        .with_tag("18")
        .start()
        .await
        .expect("start postgres:18 (is docker running?)");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    (
        c,
        format!("postgres://aethercode:aethercode@127.0.0.1:{port}/aethercode_test"),
    )
}

/// Container with the schema fully migrated.
async fn env() -> Env {
    let (c, url) = start().await;
    let db = connect(&cfg(&url, "")).await.unwrap();
    db.migrate().await.unwrap();
    Env { _c: c, url, db }
}

fn sub() -> Submission {
    Submission {
        id: Uuid::new_v4().to_string(),
        tenant_id: DEFAULT_TENANT_ID.to_owned(),
        user_id: None,
        idempotency_key: None,
        language: "python".into(),
        source_code: "print(1)".into(),
        status: Status::Queued,
        verdict: None,
        compile_stderr: String::new(),
        cpu_time_ns: 0,
        memory_bytes: 0,
        test_count: 0,
        tests_passed: 0,
        created_at: 0,
        updated_at: 0,
    }
}

fn result(i: i32, v: Verdict) -> TestResult {
    TestResult {
        test_index: i,
        verdict: v,
        cpu_time_ns: 100,
        memory_bytes: 1024 * i64::from(i + 1),
        run_time_ns: 0,
        exit_status: 0,
        sandbox_status: String::new(),
        message: String::new(),
        stdout_preview: "out".into(),
        stderr_preview: String::new(),
    }
}

async fn scalar_i64(db: &Db, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap()
}

#[tokio::test]
async fn migrate_is_idempotent() {
    let e = env().await;
    e.db.migrate().await.unwrap();
    e.db.migrate().await.unwrap();
    assert_eq!(
        scalar_i64(&e.db, "SELECT count(*) FROM schema_version").await,
        8
    );
    assert_eq!(
        scalar_i64(&e.db, "SELECT max(version)::bigint FROM schema_version").await,
        8
    );
}

#[tokio::test]
async fn embedded_list_covers_every_migration_file() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../migrations");
    let on_disk = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.unwrap().file_name().into_string().ok())
        .filter(|n| {
            std::path::Path::new(n)
                .extension()
                .is_some_and(|x| x == "sql")
        })
        .count();
    let (c, url) = start().await;
    let db = connect(&cfg(&url, "")).await.unwrap();
    db.migrate().await.unwrap();
    let applied = scalar_i64(&db, "SELECT count(*) FROM schema_version").await;
    drop(c);
    assert_eq!(usize::try_from(applied).unwrap(), on_disk);
}

#[tokio::test]
async fn concurrent_migrate_serialises_on_advisory_lock() {
    let (_c, url) = start().await;
    let db = connect(&cfg(&url, "")).await.unwrap();
    let tasks: Vec<_> = (0..6)
        .map(|_| {
            let db = db.clone();
            tokio::spawn(async move { db.migrate().await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    // A racing runner would hit the unique index on schema_version or re-run a migration.
    assert_eq!(
        scalar_i64(&db, "SELECT count(*) FROM schema_version").await,
        8
    );
    assert_eq!(
        scalar_i64(&db, "SELECT count(DISTINCT version) FROM schema_version").await,
        8
    );
}

#[tokio::test]
async fn migrates_a_database_already_migrated_by_go() {
    let (_c, url) = start().await;
    let db = connect(&cfg(&url, "")).await.unwrap();
    // What the Go runner leaves behind: migrations 1..=7 applied, versions recorded.
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../migrations");
    sqlx::raw_sql(
        "CREATE TABLE schema_version (version INTEGER NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .execute(db.pool())
    .await
    .unwrap();
    for (v, f) in [
        (1, "001_initial.sql"),
        (2, "002_step3.sql"),
        (3, "003_unique_schema_version.sql"),
        (4, "004_orphan_reaper_index.sql"),
        (5, "005_idempotency_mode.sql"),
        (6, "006_idempotency_without_mode.sql"),
        (7, "007_drop_legacy_problem_bank.sql"),
    ] {
        let sql = std::fs::read_to_string(format!("{dir}/{f}")).unwrap();
        sqlx::raw_sql(&sql).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO schema_version (version) VALUES ($1)")
            .bind(v)
            .execute(db.pool())
            .await
            .unwrap();
    }
    db.migrate().await.unwrap();
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM schema_version ORDER BY 1")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(versions, (1..=8).collect::<Vec<_>>());
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_name = 'submissions' AND column_name = 'webhook_delivered_at'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn migration_006_dedupes_cross_mode_keys() {
    let (_c, url) = start().await;
    let db = connect(&cfg(&url, "")).await.unwrap();
    db.migrate_to(5).await.unwrap();
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, tenant_id, external_subject_id) VALUES ($1, $2, 'mig006')")
        .bind(user)
        .bind(Uuid::parse_str(DEFAULT_TENANT_ID).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let (older, newer) = (Uuid::new_v4(), Uuid::new_v4());
    for (id, mode, age) in [(older, "submit", "1 minute"), (newer, "run", "0 seconds")] {
        sqlx::query(
            "INSERT INTO submissions (id, tenant_id, user_id, idempotency_key, mode, language, \
             source_code, status, created_at, updated_at) \
             VALUES ($1, $2, $3, 'k', $4, 'python', 'x', 'queued', now() - $5::interval, now())",
        )
        .bind(id)
        .bind(Uuid::parse_str(DEFAULT_TENANT_ID).unwrap())
        .bind(user)
        .bind(mode)
        .bind(age)
        .execute(db.pool())
        .await
        .unwrap();
    }
    db.migrate_to(6).await.unwrap();
    let key = |id: Uuid| {
        let pool = db.pool().clone();
        async move {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT idempotency_key FROM submissions WHERE id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert_eq!(key(older).await.as_deref(), Some("k"));
    assert_eq!(key(newer).await, None);
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_submissions_idempotency_v2'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn migration_007_drops_legacy_problem_bank() {
    let (_c, url) = start().await;
    let db = connect(&cfg(&url, "")).await.unwrap();
    let legacy = "SELECT count(*) FROM information_schema.tables \
                  WHERE table_name IN ('problems', 'problem_versions', 'test_cases')";
    db.migrate_to(6).await.unwrap();
    assert_eq!(scalar_i64(&db, legacy).await, 3);
    db.migrate_to(7).await.unwrap();
    assert_eq!(scalar_i64(&db, legacy).await, 0);
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'submissions' \
             AND column_name IN ('problem_version_id', 'mode')"
        )
        .await,
        0
    );
    assert_eq!(
        scalar_i64(
            &db,
            "SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_submissions_idempotency'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn insert_and_get_submission() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    let got = e.db.get_submission(&s.id).await.unwrap().unwrap();
    assert_eq!(got.id, s.id);
    assert_eq!(got.language, "python");
    assert_eq!(got.status, Status::Queued);
    assert_eq!(got.verdict, None);
    assert!(got.created_at > 1_700_000_000_000);
    assert!(
        e.db.get_submission(&Uuid::new_v4().to_string())
            .await
            .unwrap()
            .is_none()
    );
    assert!(e.db.get_submission("not-a-uuid").await.is_err());
}

#[tokio::test]
async fn set_status_refuses_terminal_rows() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    assert!(e.db.set_running(&s.id).await.unwrap());
    sqlx::query("UPDATE submissions SET status = 'completed' WHERE id = $1")
        .bind(Uuid::parse_str(&s.id).unwrap())
        .execute(e.db.pool())
        .await
        .unwrap();
    assert!(!e.db.set_running(&s.id).await.unwrap());
}

#[tokio::test]
async fn insert_submission_idempotent_returns_existing() {
    let e = env().await;
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, tenant_id, external_subject_id) VALUES ($1, $2, 'idem')")
        .bind(user)
        .bind(Uuid::parse_str(DEFAULT_TENANT_ID).unwrap())
        .execute(e.db.pool())
        .await
        .unwrap();
    let mut a = sub();
    a.user_id = Some(user.to_string());
    a.idempotency_key = Some("idem-key".into());
    let mut b = sub();
    b.user_id = a.user_id.clone();
    b.idempotency_key = a.idempotency_key.clone();
    let (id1, new1) = e.db.insert_submission_idempotent(&a).await.unwrap();
    let (id2, new2) = e.db.insert_submission_idempotent(&b).await.unwrap();
    assert!(new1 && !new2);
    assert_eq!(id1, a.id);
    assert_eq!(id1, id2);
}

#[tokio::test]
async fn finish_with_results_writes_verdict_and_results() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    let rs = [
        result(0, Verdict::Accepted),
        result(1, Verdict::WrongAnswer),
    ];
    e.db.finish_with_results(&s.id, Verdict::WrongAnswer, &rs)
        .await
        .unwrap();
    let got = e.db.get_submission(&s.id).await.unwrap().unwrap();
    assert_eq!(got.status, Status::Completed);
    assert_eq!(got.verdict, Some(Verdict::WrongAnswer));
    assert_eq!((got.test_count, got.tests_passed), (2, 1));
    assert_eq!((got.cpu_time_ns, got.memory_bytes), (200, 2048));
    let stored = e.db.get_test_results(&s.id).await.unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[1].verdict, Verdict::WrongAnswer);
    assert_eq!(stored[0].stdout_preview, "out");
}

#[tokio::test]
async fn finish_with_results_twice_creates_no_duplicates() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    let rs = [result(0, Verdict::Accepted), result(1, Verdict::Accepted)];
    let first =
        e.db.finish_with_results(&s.id, Verdict::Accepted, &rs)
            .await
            .unwrap();
    assert!(first);
    // Redelivery, even with a different verdict, must change nothing.
    let second =
        e.db.finish_with_results(
            &s.id,
            Verdict::RuntimeError,
            &[result(0, Verdict::RuntimeError)],
        )
        .await
        .unwrap();
    assert!(!second);
    let stored = e.db.get_test_results(&s.id).await.unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].verdict, Verdict::Accepted);
    let got = e.db.get_submission(&s.id).await.unwrap().unwrap();
    assert_eq!(got.verdict, Some(Verdict::Accepted));
    assert_eq!(got.test_count, 2);
}

#[tokio::test]
async fn finish_with_results_ignores_unknown_submission() {
    let e = env().await;
    let ghost = Uuid::new_v4().to_string();
    let r =
        e.db.finish_with_results(&ghost, Verdict::Accepted, &[result(0, Verdict::Accepted)])
            .await;
    assert!(!r.unwrap());
    assert_eq!(
        scalar_i64(&e.db, "SELECT count(*) FROM submission_test_results").await,
        0
    );
}

#[tokio::test]
async fn fail_cancel_touch_and_delete() {
    let e = env().await;
    let a = sub();
    let b = sub();
    let c = sub();
    for s in [&a, &b, &c] {
        e.db.insert_submission(s).await.unwrap();
    }
    assert!(
        e.db.fail_submission(&a.id, Verdict::CompilationError, "boom")
            .await
            .unwrap()
    );
    assert!(
        !e.db
            .fail_submission(&a.id, Verdict::InternalError, "again")
            .await
            .unwrap()
    );
    let got = e.db.get_submission(&a.id).await.unwrap().unwrap();
    assert_eq!(
        (got.status, got.verdict, got.compile_stderr.as_str()),
        (Status::Failed, Some(Verdict::CompilationError), "boom")
    );
    assert!(e.db.cancel_submission(&b.id).await.unwrap());
    assert!(!e.db.cancel_submission(&b.id).await.unwrap());
    let got = e.db.get_submission(&b.id).await.unwrap().unwrap();
    assert_eq!(got.verdict, Some(Verdict::Cancelled));
    e.db.touch_submission(&c.id).await.unwrap();
    e.db.finish_with_results(&c.id, Verdict::Accepted, &[result(0, Verdict::Accepted)])
        .await
        .unwrap();
    assert!(e.db.delete_submission(&c.id).await.unwrap());
    assert!(!e.db.delete_submission(&c.id).await.unwrap());
    assert!(e.db.get_test_results(&c.id).await.unwrap().is_empty());
}

#[tokio::test]
async fn orphaned_submissions_lists_only_stale_queued() {
    let e = env().await;
    let fresh = sub();
    let old = sub();
    e.db.insert_submission(&fresh).await.unwrap();
    e.db.insert_submission(&old).await.unwrap();
    sqlx::query("UPDATE submissions SET updated_at = now() - interval '3 minutes' WHERE id = $1")
        .bind(Uuid::parse_str(&old.id).unwrap())
        .execute(e.db.pool())
        .await
        .unwrap();
    let orphans = e.db.orphaned_submissions().await.unwrap();
    assert!(orphans.contains(&old.id));
    assert!(!orphans.contains(&fresh.id));
    e.db.touch_submission(&old.id).await.unwrap();
    assert!(!e.db.orphaned_submissions().await.unwrap().contains(&old.id));
}

#[tokio::test]
async fn mark_webhook_delivered_is_true_only_once() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    assert!(e.db.mark_webhook_delivered(&s.id).await.unwrap());
    assert!(!e.db.mark_webhook_delivered(&s.id).await.unwrap());
    assert!(
        !e.db
            .mark_webhook_delivered(&Uuid::new_v4().to_string())
            .await
            .unwrap()
    );
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let (db, id) = (e.db.clone(), sub());
            tokio::spawn(async move {
                db.insert_submission(&id).await.unwrap();
                let (a, b) = tokio::join!(
                    db.mark_webhook_delivered(&id.id),
                    db.mark_webhook_delivered(&id.id)
                );
                u8::from(a.unwrap()) + u8::from(b.unwrap())
            })
        })
        .collect();
    for h in handles {
        assert_eq!(h.await.unwrap(), 1);
    }
}

#[tokio::test]
async fn rls_blocks_cross_tenant_reads_via_app_pool() {
    let e = env().await;
    let other = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name, slug) VALUES ($1, 'Other', 'other')")
        .bind(other)
        .execute(e.db.pool())
        .await
        .unwrap();
    let mine = sub();
    let mut theirs = sub();
    theirs.tenant_id = other.to_string();
    e.db.insert_submission(&mine).await.unwrap();
    e.db.insert_submission(&theirs).await.unwrap();
    sqlx::query("ALTER ROLE aethercode_app PASSWORD 'app'")
        .execute(e.db.pool())
        .await
        .unwrap();
    let app_url = e
        .url
        .replace("aethercode:aethercode@", "aethercode_app:app@");
    let db = connect(&cfg(&e.url, &app_url)).await.unwrap();

    let ids = |mut tx: sqlx::Transaction<'static, sqlx::Postgres>| async move {
        let rows = sqlx::query("SELECT id::text FROM submissions")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        rows.iter()
            .map(|r| r.get::<String, _>(0))
            .collect::<Vec<_>>()
    };
    let seen = ids(db.app_tx(DEFAULT_TENANT_ID).await.unwrap()).await;
    assert_eq!(seen, vec![mine.id.clone()]);
    let seen = ids(db.app_tx(&other.to_string()).await.unwrap()).await;
    assert_eq!(seen, vec![theirs.id.clone()]);
    // No tenant set at all: nothing is visible.
    let rows = sqlx::query("SELECT id FROM submissions")
        .fetch_all(db.app_pool())
        .await
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn app_pool_falls_back_to_main_pool_without_url() {
    let e = env().await;
    assert_eq!(
        scalar_i64(&Db::clone(&e.db), "SELECT count(*) FROM schema_version").await,
        8
    );
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM schema_version")
        .fetch_one(e.db.app_pool())
        .await
        .unwrap();
    assert_eq!(n, 8);
}

#[test]
fn truncate_respects_char_boundaries() {
    assert_eq!(truncate("hello", 10), "hello");
    assert_eq!(truncate("hello", 3), "hel");
    assert_eq!(truncate("héllo", 2), "h");
    assert_eq!(truncate("héllo", 3), "hé");
    assert_eq!(truncate("", 0), "");
}

#[tokio::test]
async fn db_futures_are_send_for_worker_tasks() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    let db = e.db.clone();
    let id = s.id.clone();
    tokio::spawn(async move {
        db.finish_with_results(&id, Verdict::Accepted, &[result(0, Verdict::Accepted)])
            .await
            .unwrap();
        db.delete_submission(&id).await.unwrap();
        db.app_tx(DEFAULT_TENANT_ID).await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn finish_after_cancel_writes_no_results() {
    let e = env().await;
    let s = sub();
    e.db.insert_submission(&s).await.unwrap();
    assert!(e.db.cancel_submission(&s.id).await.unwrap());
    let finished =
        e.db.finish_with_results(&s.id, Verdict::Accepted, &[result(0, Verdict::Accepted)])
            .await
            .unwrap();
    assert!(!finished);
    assert!(e.db.get_test_results(&s.id).await.unwrap().is_empty());
    let got = e.db.get_submission(&s.id).await.unwrap().unwrap();
    assert_eq!(
        (got.status, got.verdict),
        (Status::Failed, Some(Verdict::Cancelled))
    );
}
