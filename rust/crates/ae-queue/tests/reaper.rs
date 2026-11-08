mod common;

use ae_core::model::{Checker, DEFAULT_TENANT_ID, Job, Limits, Status, Submission, Verdict};
use ae_queue::TestSet;
use common::{cfg, env, now_ms};
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

async fn pg() -> (ContainerAsync<Postgres>, ae_db::Db) {
    let c = Postgres::default()
        .with_user("aethercode")
        .with_password("aethercode")
        .with_db_name("aethercode_test")
        .with_tag("18")
        .start()
        .await
        .expect("start postgres:18");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://aethercode:aethercode@127.0.0.1:{port}/aethercode_test");
    let m = HashMap::from([("AC_DATABASE_URL".to_owned(), url)]);
    let db = ae_db::connect(&ae_core::Config::from_map(&m).unwrap())
        .await
        .unwrap();
    db.migrate().await.unwrap();
    (c, db)
}

fn sub(id: &str) -> Submission {
    Submission {
        id: id.into(),
        tenant_id: DEFAULT_TENANT_ID.into(),
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

#[tokio::test]
async fn reaper_reenqueues_stuck_queued_submissions_on_the_run_stream() {
    let (_pg, db) = pg().await;
    let e = env().await;
    let stuck = Uuid::new_v4().to_string();
    let fresh = Uuid::new_v4().to_string();
    db.insert_submission(&sub(&stuck)).await.unwrap();
    db.insert_submission(&sub(&fresh)).await.unwrap();
    sqlx::query(
        "UPDATE submissions SET updated_at = now() - interval '5 minutes' WHERE id = $1::uuid",
    )
    .bind(&stuck)
    .execute(db.pool())
    .await
    .unwrap();

    assert_eq!(e.q.reap_orphans_once(&db).await.unwrap(), 1);
    let d = e.q.read_batch().await.unwrap();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].job_id, stuck);
    assert_eq!(d[0].stream, "ac:submissions:run");
    assert_eq!(d[0].expires_at, None);
    // updated_at was touched, so the next pass leaves it alone.
    assert_eq!(e.q.reap_orphans_once(&db).await.unwrap(), 0);
}

#[tokio::test]
async fn expired_job_marks_submission_failed_when_db_attached() {
    let (_pg, db) = pg().await;
    let e = env().await;
    let q = ae_queue::Queue::connect(&cfg(&e.addr, &[]))
        .await
        .unwrap()
        .with_db(db.clone());
    let id = Uuid::new_v4().to_string();
    db.insert_submission(&sub(&id)).await.unwrap();
    let job = Job {
        job_id: id.clone(),
        submission_id: id.clone(),
        language: "python".into(),
        source: String::new(),
        limits: Limits::default(),
        checker: Checker::Exact,
        tests_key: format!("ac:tests:{id}"),
        expires_at: Some(now_ms() - 1),
    };
    q.prepare_and_enqueue(&job, &TestSet::default(), "ac:submissions:run")
        .await
        .unwrap();
    assert!(q.read_batch().await.unwrap().is_empty());
    let s = db.get_submission(&id).await.unwrap().unwrap();
    assert_eq!(s.status, Status::Failed);
    assert_eq!(s.verdict, Some(Verdict::InternalError));
    // The reaper only looks at queued rows, so it cannot resurrect the job.
    sqlx::query(
        "UPDATE submissions SET updated_at = now() - interval '5 minutes' WHERE id = $1::uuid",
    )
    .bind(&id)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(q.reap_orphans_once(&db).await.unwrap(), 0);
}
