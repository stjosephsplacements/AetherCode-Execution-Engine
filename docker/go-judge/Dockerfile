# go-judge sandbox image with the languages the engine runs:
# C, C++, Java 21, Go, Python 3, JavaScript (Node), SQLite3, Bash, Rust, C#
# (Mono), PHP, Ruby, and Kotlin (pinned kotlinc below). TypeScript (bun) has
# no Debian package; until a pinned release is added, the startup probe reports
# it unavailable (GET /api/v1/languages) and rejects it.
#
# Base image pinned to v1.13.0 (the version the engine is validated against).
# To update: docker pull criyle/go-judge:<new-tag> && docker inspect --format='{{.RepoDigests}}' criyle/go-judge:<new-tag>
FROM criyle/go-judge:v1.13.0 AS go-judge

# Debian 13 (trixie): openjdk-21 is not packaged for bookworm.
FROM debian:trixie-slim

# Pin JetBrains Kotlin compiler (no Debian package). Bump with the release tag.
ARG KOTLIN_VERSION=2.1.21
ARG KOTLIN_SHA256=1ba08a8b45da99339a0601134cc037b54cf85e9bc0edbe76dcbd27c2d684a977

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl unzip \
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
    && curl -fsSL \
         "https://github.com/JetBrains/kotlin/releases/download/v${KOTLIN_VERSION}/kotlin-compiler-${KOTLIN_VERSION}.zip" \
         -o /tmp/kotlin.zip \
    && echo "${KOTLIN_SHA256}  /tmp/kotlin.zip" | sha256sum -c - \
    && unzip -q /tmp/kotlin.zip -d /tmp \
    && mv /tmp/kotlinc /usr/share/kotlin \
    && ln -sf /usr/share/kotlin/bin/kotlinc /usr/bin/kotlinc \
    && ln -sf /usr/share/kotlin/bin/kotlin /usr/bin/kotlin \
    && rm -rf /tmp/kotlin.zip /var/lib/apt/lists/* \
    && apt-get purge -y --auto-remove curl unzip \
    && kotlinc -version

WORKDIR /opt
COPY --from=go-judge /opt/go-judge /opt/go-judge
COPY mount.yaml /opt/mount.yaml
EXPOSE 5050
# go-judge must run as root (it creates cgroups + namespaces for each sandbox).
ENTRYPOINT ["/opt/go-judge"]
