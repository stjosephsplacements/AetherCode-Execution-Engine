//! Load generator for the AetherCode execution API (port of `cmd/loadgen`). Works against
//! any engine that serves `POST /api/v1/execute` and `GET /api/v1/stream`.
//!
//! Each simulated student submits, waits for the terminal VERDICT on the SSE stream,
//! thinks, and repeats until the duration ends. Latency is submit-to-VERDICT.
//!
//! Cold vs warm: the engine's verdict payload carries no compile-cache flag, so a
//! submission is classified by the loadgen itself. The first submission of a
//! `(language, source)` pair is cold (the engine must compile it); every later one is warm
//! (the compile cache should hold it). Interpreted languages follow the same rule, which
//! keeps the split uniform across the mix. A pair counts as seen only once its POST was
//! accepted.
//!
//! Caveats: students that submit the same new pair at the same moment all count cold or
//! all but the first count warm, depending on arrival order, so warm can be slightly
//! inflated while the engine is still compiling the first copy (the engine collapses the
//! concurrent compiles into one). The engine's compile cache survives between runs, so
//! restart the engine between repeats or the next run starts warm and its cold numbers are
//! meaningless. POST retries (429/5xx) are counted in `post_retries`, are not errors, and
//! are excluded from the ingestion latency (but not from submit-to-verdict).

mod client;
mod report;
mod templates;

pub use client::{Config, run};
pub use report::{Report, Sample, Stats, build_report, percentile};
pub use templates::{Template, TestInput, pick, supported};
