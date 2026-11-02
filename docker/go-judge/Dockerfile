# go-judge sandbox image with the languages the engine runs:
# C, C++, Java 21, Go, Python 3, JavaScript (Node), SQLite3, Bash, Rust, C#
# (Mono), PHP and Ruby. Kotlin (kotlinc) and TypeScript (bun) have no Debian
# package; until a pinned release is added here, the engine's startup probe
# reports them as unavailable (GET /api/v1/languages) and rejects them.
#
# Base image pinned to v1.13.0 (the version the engine is validated against).
# To update: docker pull criyle/go-judge:<new-tag> && docker inspect --format='{{.RepoDigests}}' criyle/go-judge:<new-tag>
FROM criyle/go-judge:v1.13.0 AS go-judge

# Debian 13 (trixie): openjdk-21 is not packaged for bookworm.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ make \
      python3 \
      nodejs \
      openjdk-21-jdk-headless \
      golang \
      sqlite3 \
      rustc \
      mono-mcs mono-runtime \
      php-cli \
      ruby \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
COPY --from=go-judge /opt/go-judge /opt/go-judge
COPY mount.yaml /opt/mount.yaml
EXPOSE 5050
# go-judge must run as root (it creates cgroups + namespaces for each sandbox).
ENTRYPOINT ["/opt/go-judge"]
