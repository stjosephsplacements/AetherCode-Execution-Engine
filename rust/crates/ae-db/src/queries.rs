use crate::{Db, Error, Result};
use ae_core::model::{Status, Submission, TestResult, Verdict};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

const PREVIEW_BYTES: usize = 256;

/// Cuts `s` to at most `max_bytes` without splitting a character.
#[must_use]
pub fn truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut cut = max_bytes;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    &s[..cut]
}

fn id(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| Error::InvalidId(s.to_owned()))
}

fn opt_id(s: Option<&str>) -> Result<Option<Uuid>> {
    s.map(id).transpose()
}

fn wire<T: serde::Serialize>(v: T) -> Result<String> {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => Ok(s),
        other => Err(Error::Value(format!("{other:?}"))),
    }
}

fn unwire<T: serde::de::DeserializeOwned>(s: String) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(s.clone())).map_err(|_| Error::Value(s))
}

const SUB_COLS: &str = "id::text, tenant_id::text, user_id::text, idempotency_key, language, \
    source_code, status, verdict, compile_stderr, cpu_time_ns, memory_bytes, test_count, \
    tests_passed, (extract(epoch from created_at) * 1000)::bigint AS created_ms, \
    (extract(epoch from updated_at) * 1000)::bigint AS updated_ms";

fn submission(r: &PgRow) -> Result<Submission> {
    Ok(Submission {
        id: r.try_get("id")?,
        tenant_id: r.try_get("tenant_id")?,
        user_id: r.try_get("user_id")?,
        idempotency_key: r.try_get("idempotency_key")?,
        language: r.try_get("language")?,
        source_code: r.try_get("source_code")?,
        status: unwire(r.try_get("status")?)?,
        verdict: r
            .try_get::<Option<String>, _>("verdict")?
            .map(unwire)
            .transpose()?,
        compile_stderr: r
            .try_get::<Option<String>, _>("compile_stderr")?
            .unwrap_or_default(),
        cpu_time_ns: r.try_get::<Option<i64>, _>("cpu_time_ns")?.unwrap_or(0),
        memory_bytes: r.try_get::<Option<i64>, _>("memory_bytes")?.unwrap_or(0),
        test_count: r.try_get::<Option<i32>, _>("test_count")?.unwrap_or(0),
        tests_passed: r.try_get::<Option<i32>, _>("tests_passed")?.unwrap_or(0),
        created_at: r.try_get("created_ms")?,
        updated_at: r.try_get("updated_ms")?,
    })
}

const INSERT: &str = "INSERT INTO submissions (id, tenant_id, user_id, idempotency_key, \
    language, source_code, status, created_at, updated_at) \
    VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())";

impl Db {
    /// # Errors
    /// Fails on a malformed id or a database error (including FK or unique violations).
    pub async fn insert_submission(&self, s: &Submission) -> Result<()> {
        sqlx::query(INSERT)
            .bind(id(&s.id)?)
            .bind(id(&s.tenant_id)?)
            .bind(opt_id(s.user_id.as_deref())?)
            .bind(&s.idempotency_key)
            .bind(&s.language)
            .bind(&s.source_code)
            .bind(wire(s.status)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Inserts unless `(tenant, user, key)` already exists. Returns the surviving id and
    /// whether this call created it.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn insert_submission_idempotent(&self, s: &Submission) -> Result<(String, bool)> {
        let sql = format!(
            "{INSERT} ON CONFLICT (tenant_id, user_id, idempotency_key) \
             WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id::text"
        );
        let inserted: Option<String> = sqlx::query_scalar(&sql)
            .bind(id(&s.id)?)
            .bind(id(&s.tenant_id)?)
            .bind(opt_id(s.user_id.as_deref())?)
            .bind(&s.idempotency_key)
            .bind(&s.language)
            .bind(&s.source_code)
            .bind(wire(s.status)?)
            .fetch_optional(&self.pool)
            .await?;
        if let Some(new_id) = inserted {
            return Ok((new_id, true));
        }
        let existing: String = sqlx::query_scalar(
            "SELECT id::text FROM submissions \
             WHERE tenant_id = $1 AND user_id = $2 AND idempotency_key = $3",
        )
        .bind(id(&s.tenant_id)?)
        .bind(opt_id(s.user_id.as_deref())?)
        .bind(&s.idempotency_key)
        .fetch_one(&self.pool)
        .await?;
        Ok((existing, false))
    }

    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn get_submission(&self, sub_id: &str) -> Result<Option<Submission>> {
        let row = sqlx::query(&format!("SELECT {SUB_COLS} FROM submissions WHERE id = $1"))
            .bind(id(sub_id)?)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(submission).transpose()
    }

    /// Hard-deletes a submission and its test results in one transaction.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn delete_submission(&self, sub_id: &str) -> Result<bool> {
        let sid = id(sub_id)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM submission_test_results WHERE submission_id = $1")
            .bind(sid)
            .execute(&mut *tx)
            .await?;
        let n = sqlx::query("DELETE FROM submissions WHERE id = $1")
            .bind(sid)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(n > 0)
    }

    /// Moves a non-terminal submission to `status`. False when it is missing or terminal.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn set_status(&self, sub_id: &str, status: Status) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE submissions SET status = $2, updated_at = now() \
             WHERE id = $1 AND status NOT IN ('completed', 'failed')",
        )
        .bind(id(sub_id)?)
        .bind(wire(status)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    /// # Errors
    /// See [`Db::set_status`].
    pub async fn set_running(&self, sub_id: &str) -> Result<bool> {
        self.set_status(sub_id, Status::Running).await
    }

    /// Bumps `updated_at` so the orphan reaper does not re-enqueue a live job.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn touch_submission(&self, sub_id: &str) -> Result<()> {
        sqlx::query("UPDATE submissions SET updated_at = now() WHERE id = $1")
            .bind(id(sub_id)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Writes the results and the terminal state in one transaction. Redelivery is a
    /// no-op: results conflict on `(submission_id, test_index)` and the update only
    /// touches non-terminal rows.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error; nothing is written then.
    pub async fn finish_with_results(
        &self,
        sub_id: &str,
        verdict: Verdict,
        results: &[TestResult],
    ) -> Result<()> {
        let sid = id(sub_id)?;
        let passed = results
            .iter()
            .filter(|r| r.verdict == Verdict::Accepted)
            .count();
        let cpu: i64 = results.iter().map(|r| r.cpu_time_ns).sum();
        let peak = results.iter().map(|r| r.memory_bytes).max().unwrap_or(0);
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE submissions SET status = 'completed', verdict = $2, compile_stderr = '', \
             cpu_time_ns = $3, memory_bytes = $4, test_count = $5, tests_passed = $6, \
             updated_at = now() WHERE id = $1 AND status NOT IN ('completed', 'failed')",
        )
        .bind(sid)
        .bind(wire(verdict)?)
        .bind(cpu)
        .bind(peak)
        .bind(i32::try_from(results.len()).map_err(|_| Error::Value("too many results".into()))?)
        .bind(i32::try_from(passed).map_err(|_| Error::Value("too many results".into()))?)
        .execute(&mut *tx)
        .await?;
        if !results.is_empty() {
            let verdicts = results
                .iter()
                .map(|r| wire(r.verdict))
                .collect::<Result<Vec<_>>>()?;
            sqlx::query(
                "INSERT INTO submission_test_results \
                 (submission_id, test_index, verdict, cpu_time_ns, memory_bytes, \
                  stdout_preview, stderr_preview) \
                 SELECT $1, * FROM UNNEST($2::int[], $3::text[], $4::bigint[], $5::bigint[], \
                                          $6::text[], $7::text[]) \
                 ON CONFLICT (submission_id, test_index) DO NOTHING",
            )
            .bind(sid)
            .bind(results.iter().map(|r| r.test_index).collect::<Vec<_>>())
            .bind(verdicts)
            .bind(results.iter().map(|r| r.cpu_time_ns).collect::<Vec<_>>())
            .bind(results.iter().map(|r| r.memory_bytes).collect::<Vec<_>>())
            .bind(
                results
                    .iter()
                    .map(|r| truncate(&r.stdout_preview, PREVIEW_BYTES))
                    .collect::<Vec<_>>(),
            )
            .bind(
                results
                    .iter()
                    .map(|r| truncate(&r.stderr_preview, PREVIEW_BYTES))
                    .collect::<Vec<_>>(),
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Marks a non-terminal submission failed with `verdict` and `compile_stderr`
    /// (truncated to 4096 bytes). False when it was already terminal.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn fail_submission(
        &self,
        sub_id: &str,
        verdict: Verdict,
        stderr: &str,
    ) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE submissions SET status = 'failed', verdict = $2, compile_stderr = $3, \
             cpu_time_ns = 0, memory_bytes = 0, test_count = 0, tests_passed = 0, \
             updated_at = now() WHERE id = $1 AND status NOT IN ('completed', 'failed')",
        )
        .bind(id(sub_id)?)
        .bind(wire(verdict)?)
        .bind(truncate(stderr, 4096))
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    /// Test results ordered by index. Fields the table does not store come back zeroed.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn get_test_results(&self, sub_id: &str) -> Result<Vec<TestResult>> {
        let rows = sqlx::query(
            "SELECT test_index, verdict, cpu_time_ns, memory_bytes, stdout_preview, \
             stderr_preview FROM submission_test_results \
             WHERE submission_id = $1 ORDER BY test_index",
        )
        .bind(id(sub_id)?)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(TestResult {
                    test_index: r.try_get("test_index")?,
                    verdict: unwire(r.try_get("verdict")?)?,
                    cpu_time_ns: r.try_get::<Option<i64>, _>("cpu_time_ns")?.unwrap_or(0),
                    memory_bytes: r.try_get::<Option<i64>, _>("memory_bytes")?.unwrap_or(0),
                    run_time_ns: 0,
                    exit_status: 0,
                    sandbox_status: String::new(),
                    message: String::new(),
                    stdout_preview: r
                        .try_get::<Option<String>, _>("stdout_preview")?
                        .unwrap_or_default(),
                    stderr_preview: r
                        .try_get::<Option<String>, _>("stderr_preview")?
                        .unwrap_or_default(),
                })
            })
            .collect()
    }

    /// Queued submissions untouched for over two minutes (oldest first, at most 100).
    ///
    /// # Errors
    /// Fails on a database error.
    pub async fn orphaned_submissions(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT id::text FROM submissions \
             WHERE status = 'queued' AND updated_at < now() - interval '2 minutes' \
             ORDER BY updated_at LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Cancels a queued submission. False when it is missing or no longer queued.
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn cancel_submission(&self, sub_id: &str) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE submissions SET status = 'failed', verdict = 'cancelled', updated_at = now() \
             WHERE id = $1 AND status = 'queued'",
        )
        .bind(id(sub_id)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    /// True only for the call that first records webhook delivery (atomic).
    ///
    /// # Errors
    /// Fails on a malformed id or a database error.
    pub async fn mark_webhook_delivered(&self, sub_id: &str) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE submissions SET webhook_delivered_at = now() \
             WHERE id = $1 AND webhook_delivered_at IS NULL",
        )
        .bind(id(sub_id)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }
}
