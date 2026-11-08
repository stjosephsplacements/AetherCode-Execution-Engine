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
//! keeps the split uniform across the mix.

mod client;
mod report;
mod templates;

pub use client::{Config, run};
pub use report::{Report, Sample, Stats, build_report, percentile};
pub use templates::{Template, TestInput, pick, supported};
