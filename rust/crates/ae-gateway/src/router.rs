//! The public router and the server entry point.

use crate::handlers::{self, AppState};
use crate::middleware::RequestLogLayer;
use crate::{metrics, sse};
use axum::Router;
use axum::routing::{get, post};
use std::future::Future;
use std::net::SocketAddr;

/// Every public route. Layers, outermost first: request log, HTTP metrics, CORS, auth,
/// rate limit; admission runs inside the execute handler, where the priority stream
/// and the job's cost are known. `/livez` and `/readyz` are public (probes carry no
/// token); `/metrics` is only on [`metrics::metrics_router`].
pub fn router(state: AppState) -> Router {
    let layers = tower::ServiceBuilder::new()
        .layer(RequestLogLayer::new())
        .layer(axum::middleware::from_fn(metrics::record_http))
        .layer(state.cors.clone())
        .layer(state.auth.clone())
        .layer(state.rate_limit.clone());
    Router::new()
        .route("/api/v1/execute", post(handlers::execute))
        .route("/api/v1/stream", get(sse::stream))
        .route("/api/v1/health", get(handlers::health))
        .route("/api/v1/languages", get(handlers::languages))
        .route(
            "/api/v1/submissions/{id}",
            get(handlers::get_submission).delete(handlers::cancel),
        )
        .route("/livez", get(metrics::livez))
        .route("/readyz", get(handlers::readyz))
        .with_state(state)
        .layer(layers)
}

/// Serves `app` on `listener` until `shutdown` resolves, recording each connection's
/// peer address (`ConnectInfo<SocketAddr>`), which the per-IP rate limiter requires.
///
/// # Errors
/// Fails when accepting connections fails.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}
