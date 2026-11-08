use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

/// One simulated submission.
#[derive(Debug, Clone)]
pub struct Sample {
    pub language: String,
    pub expected: String,
    /// Terminal verdict; `None` when the job was lost.
    pub actual: Option<String>,
    pub ingest_ms: f64,
    pub verdict_ms: f64,
    pub cold: bool,
    /// POST attempts that were answered 429 or 5xx and retried.
    pub retries: u32,
    pub error: Option<String>,
}

/// Linear-interpolated percentile of an ascending slice; NaN when empty (Go's `percentile`).
#[must_use]
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    #[allow(clippy::cast_precision_loss)]
    let idx = p / 100.0 * (sorted.len() - 1) as f64;
    // idx is within 0..len, so the casts are exact.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let (lo, hi) = (idx.floor() as usize, idx.ceil() as usize);
    if lo == hi || hi >= sorted.len() {
        return sorted[lo];
    }
    let frac = idx - idx.floor();
    sorted[lo] * (1.0 - frac) + sorted[hi] * frac
}

/// Latency summary in ms. Percentiles are `None` (JSON null) when there are no samples.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Stats {
    pub count: usize,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

impl Stats {
    fn of(mut v: Vec<f64>) -> Self {
        v.sort_by(f64::total_cmp);
        let p = |q| Some(percentile(&v, q)).filter(|x| !x.is_nan());
        Self {
            count: v.len(),
            p50_ms: p(50.0),
            p95_ms: p(95.0),
            p99_ms: p(99.0),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub total: usize,
    pub completed: usize,
    pub lost: usize,
    pub mismatched: usize,
    /// lost / total
    pub error_rate: f64,
    /// mismatched / completed
    pub mismatch_rate: f64,
    /// Total POST retries after 429/5xx. Reported apart from `error_rate`: a retried
    /// submission that then succeeds is not an error and its retry wait is not in `ingest`.
    pub post_retries: usize,
    /// Submissions that needed at least one retry.
    pub retried_submissions: usize,
    pub wall_time_ms: u128,
    pub throughput: f64,
    pub verdict_rate: f64,
    pub ingest: Stats,
    /// Submit-to-VERDICT, all completed submissions.
    pub verdict: Stats,
    /// Submit-to-VERDICT, first submission of a (language, source) pair.
    pub verdict_cold: Stats,
    /// Submit-to-VERDICT, repeats of a pair already seen.
    pub verdict_warm: Stats,
    pub verdicts: BTreeMap<String, usize>,
    pub languages: BTreeMap<String, usize>,
    pub errors: BTreeMap<String, usize>,
}

fn rate(n: usize, d: usize) -> f64 {
    if d == 0 {
        0.0
    } else {
        #[allow(clippy::cast_precision_loss)]
        {
            n as f64 / d as f64
        }
    }
}

#[must_use]
pub fn build_report(samples: &[Sample], wall: Duration) -> Report {
    let mut r = Report {
        total: samples.len(),
        completed: 0,
        lost: 0,
        mismatched: 0,
        error_rate: 0.0,
        mismatch_rate: 0.0,
        post_retries: 0,
        retried_submissions: 0,
        wall_time_ms: wall.as_millis(),
        throughput: 0.0,
        verdict_rate: 0.0,
        ingest: Stats::of(vec![]),
        verdict: Stats::of(vec![]),
        verdict_cold: Stats::of(vec![]),
        verdict_warm: Stats::of(vec![]),
        verdicts: BTreeMap::new(),
        languages: BTreeMap::new(),
        errors: BTreeMap::new(),
    };
    let (mut ing, mut all, mut cold, mut warm) = (vec![], vec![], vec![], vec![]);
    for s in samples {
        *r.languages.entry(s.language.clone()).or_default() += 1;
        r.post_retries += s.retries as usize;
        r.retried_submissions += usize::from(s.retries > 0);
        let Some(actual) = &s.actual else {
            r.lost += 1;
            *r.errors
                .entry(s.error.clone().unwrap_or_else(|| "unknown".into()))
                .or_default() += 1;
            continue;
        };
        r.completed += 1;
        *r.verdicts.entry(actual.clone()).or_default() += 1;
        if *actual != s.expected {
            r.mismatched += 1;
        }
        ing.push(s.ingest_ms);
        all.push(s.verdict_ms);
        if s.cold { &mut cold } else { &mut warm }.push(s.verdict_ms);
    }
    r.ingest = Stats::of(ing);
    r.verdict = Stats::of(all);
    r.verdict_cold = Stats::of(cold);
    r.verdict_warm = Stats::of(warm);
    r.error_rate = rate(r.lost, r.total);
    r.mismatch_rate = rate(r.mismatched, r.completed);
    let secs = wall.as_secs_f64();
    if secs > 0.0 {
        #[allow(clippy::cast_precision_loss)]
        {
            r.throughput = r.total as f64 / secs;
            r.verdict_rate = r.completed as f64 / secs;
        }
    }
    r
}

fn cell(v: Option<f64>) -> String {
    v.map_or_else(|| "      -".into(), |x| format!("{x:7.1}"))
}

impl Report {
    #[must_use]
    pub fn json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// Human-readable report.
    #[must_use]
    pub fn table(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(o, "AetherCode load test results");
        let _ = writeln!(o, "  Total submissions:  {}", self.total);
        let _ = writeln!(o, "  Completed:          {}", self.completed);
        let _ = writeln!(
            o,
            "  Lost:               {} ({:.2}% error rate)",
            self.lost,
            self.error_rate * 100.0
        );
        let _ = writeln!(
            o,
            "  Verdict mismatch:   {} ({:.2}%)",
            self.mismatched,
            self.mismatch_rate * 100.0
        );
        let _ = writeln!(
            o,
            "  POST retries:       {} in {} submissions (429/5xx, not counted as errors)",
            self.post_retries, self.retried_submissions
        );
        let _ = writeln!(o, "  Wall time:          {} ms", self.wall_time_ms);
        let _ = writeln!(
            o,
            "  Throughput:         {:.1} submissions/s",
            self.throughput
        );
        let _ = writeln!(
            o,
            "  Verdict rate:       {:.1} verdicts/s",
            self.verdict_rate
        );
        let _ = writeln!(o);
        let _ = writeln!(o, "  Latency (ms)            n      p50      p95      p99");
        for (name, s) in [
            ("Ingestion", &self.ingest),
            ("Verdict (all)", &self.verdict),
            ("Verdict COLD", &self.verdict_cold),
            ("Verdict WARM", &self.verdict_warm),
        ] {
            let _ = writeln!(
                o,
                "  {name:<18}{:>6} {} {} {}",
                s.count,
                cell(s.p50_ms),
                cell(s.p95_ms),
                cell(s.p99_ms)
            );
        }
        for (title, m) in [
            ("Verdicts", &self.verdicts),
            ("Languages", &self.languages),
            ("Errors", &self.errors),
        ] {
            if m.is_empty() {
                continue;
            }
            let _ = writeln!(o, "\n  {title}:");
            for (k, n) in m {
                let _ = writeln!(o, "    {k:<40} {n:>6}");
            }
        }
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(cold: bool, ms: f64) -> Sample {
        Sample {
            language: "c".into(),
            expected: "accepted".into(),
            actual: Some("accepted".into()),
            ingest_ms: 1.0,
            verdict_ms: ms,
            cold,
            retries: 0,
            error: None,
        }
    }

    #[test]
    fn loadgen_report_separates_cold_and_warm() {
        let mut v = vec![s(true, 900.0), s(true, 1100.0)];
        v.extend((1..=100).map(|i| s(false, f64::from(i))));
        v.push(Sample {
            actual: None,
            error: Some("SSE timeout".into()),
            ..s(false, 0.0)
        });
        let r = build_report(&v, Duration::from_secs(10));
        assert_eq!((r.verdict_cold.count, r.verdict_warm.count), (2, 100));
        assert_eq!(r.verdict_cold.p50_ms, Some(1000.0));
        assert_eq!(r.verdict_warm.p50_ms, Some(50.5));
        assert_eq!(r.verdict.count, 102);
        assert_eq!((r.total, r.completed, r.lost), (103, 102, 1));
        assert!((r.error_rate - 1.0 / 103.0).abs() < 1e-12);
        let t = r.table();
        assert!(t.contains("Verdict COLD") && t.contains("Verdict WARM"));
        let j: serde_json::Value = serde_json::from_str(&r.json()).unwrap();
        assert_eq!(j["verdict_cold"]["count"], 2);
        assert_eq!(j["verdict_warm"]["count"], 100);
    }

    #[test]
    fn retries_are_reported_apart_from_errors() {
        let v = vec![
            Sample {
                retries: 3,
                ..s(false, 10.0)
            },
            Sample {
                retries: 1,
                ..s(false, 10.0)
            },
            s(false, 10.0),
        ];
        let r = build_report(&v, Duration::from_secs(1));
        assert_eq!((r.post_retries, r.retried_submissions), (4, 2));
        assert_eq!((r.lost, r.error_rate.to_bits()), (0, 0.0f64.to_bits()));
        assert!(r.table().contains("POST retries:       4 in 2 submissions"));
    }

    #[test]
    fn percentile_math_matches_go_loadgen() {
        assert!(percentile(&[], 50.0).is_nan());
        assert!((percentile(&[7.0], 99.0) - 7.0).abs() < 1e-12);
        let v: Vec<f64> = (1..=5).map(f64::from).collect();
        assert!((percentile(&v, 50.0) - 3.0).abs() < 1e-12);
        assert!((percentile(&v, 0.0) - 1.0).abs() < 1e-12);
        assert!((percentile(&v, 100.0) - 5.0).abs() < 1e-12);
        // idx = 0.25*3 = 0.75 -> 10*0.25 + 20*0.75
        assert!((percentile(&[10.0, 20.0, 30.0, 40.0], 25.0) - 17.5).abs() < 1e-12);
        let h: Vec<f64> = (1..=100).map(f64::from).collect();
        assert!((percentile(&h, 95.0) - 95.05).abs() < 1e-9);
        assert!((percentile(&h, 99.0) - 99.01).abs() < 1e-9);
    }
}
