# go-judge sandbox image with the 7 languages the engine supports:
# C, C++, Java 21, Go, Python 3, JavaScript (Node), SQLite3.
#
# Base image pinned to v1.13.0 (the version the engine is validated against).
# To update: docker pull criyle/go-judge:<new-tag> && docker inspect --format='{{.RepoDigests}}' criyle/go-judge:<new-tag>
FROM criyle/go-judge:v1.13.0 AS go-judge

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ make \
      python3 \
      nodejs \
      openjdk-21-jdk-headless \
      golang \
      sqlite3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
COPY --from=go-judge /opt/go-judge /opt/go-judge
COPY mount.yaml /opt/mount.yaml
EXPOSE 5050
# go-judge must run as root (it creates cgroups + namespaces for each sandbox).
ENTRYPOINT ["/opt/go-judge"]
