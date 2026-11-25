# syntax=docker/dockerfile:1
#
# Rust execution engine image. Build from the repository root (migrations/ is
# embedded at compile time):
#
#   docker build -f Dockerfile.rs --build-arg COMMIT_SHA=$(git rev-parse HEAD) \
#     -t aethercode-exec-rs:dev .

# ---- Dependency planner (cargo-chef): recipe.json only changes when manifests change ----
# Pinned by digest; update it deliberately (docker buildx imagetools inspect <tag>).
FROM lukemathwalker/cargo-chef:latest-rust-1.100.0-bookworm@sha256:9573268a2a874efc1c755a0094a50c25988b2c6a0b7a520ddb7fd08d868d1c43 AS chef
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ ./crates/
COPY third_party/ ./third_party/
RUN cargo chef prepare --recipe-path recipe.json

# ---- Build: cached dependency layer, then the workspace ----
FROM chef AS build
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_TERM_COLOR=never
COPY --from=planner /src/recipe.json recipe.json
COPY third_party/ ./third_party/
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
RUN cargo chef cook --release --locked --recipe-path recipe.json -p ae-engine
COPY Cargo.toml Cargo.lock rust-toolchain.toml deny.toml ./
COPY crates/ ./crates/
COPY third_party/ ./third_party/
COPY migrations/ ./migrations/
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
# Metrics, /livez and /readyz are served on ${AC_DEBUG_ADDR:-127.0.0.1}:${AC_DEBUG_PORT:-6060} (not published).
EXPOSE 5100
ENTRYPOINT ["/usr/local/bin/engine"]
