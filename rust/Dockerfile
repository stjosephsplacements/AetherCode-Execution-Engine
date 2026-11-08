# syntax=docker/dockerfile:1
#
# Rust execution engine image. Build from the repository root (the migrations live in
# ../migrations relative to the workspace and are embedded at compile time):
#
#   docker build -f rust/Dockerfile --build-arg COMMIT_SHA=$(git rev-parse HEAD) \
#     -t aethercode-exec-rs:dev .

# ---- Dependency planner (cargo-chef): recipe.json only changes when manifests change ----
FROM lukemathwalker/cargo-chef:latest-rust-1.99.1-bookworm AS chef
WORKDIR /src/rust

FROM chef AS planner
COPY rust/ ./
RUN cargo chef prepare --recipe-path recipe.json

# ---- Build: cached dependency layer, then the workspace ----
FROM chef AS build
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_TERM_COLOR=never
COPY --from=planner /src/rust/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json -p ae-engine
COPY rust/ ./
COPY migrations/ /src/migrations/
RUN cargo build --release --locked -p ae-engine --bin engine \
    && cp target/release/engine /engine

# ---- Runtime: distroless (glibc + CA certs, no shell, no curl), non-root ----
FROM gcr.io/distroless/cc-debian12:nonroot
ARG COMMIT_SHA=dev
LABEL org.opencontainers.image.title="aethercode-exec-rs" \
      org.opencontainers.image.source="https://github.com/stjosephsplacements/AetherCode-Execution-Engine" \
      org.opencontainers.image.revision="${COMMIT_SHA}"
COPY --from=build /engine /usr/local/bin/engine
USER 65532:65532
# 5100 = HTTP gateway + SSE (/livez, /readyz, /api/v1/*).
# Metrics are served on 127.0.0.1:${AC_DEBUG_PORT:-6060} only (loopback; not published).
EXPOSE 5100
ENTRYPOINT ["/usr/local/bin/engine"]
