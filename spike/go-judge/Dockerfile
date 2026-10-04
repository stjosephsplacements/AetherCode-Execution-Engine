# Spike image: go-judge sandbox server + compilers/runtimes for C, C++, Python, Java, JS.
FROM criyle/go-judge:latest AS go-judge

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ python3 openjdk-17-jdk-headless nodejs \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
COPY --from=go-judge /opt/go-judge /opt/go-judge
EXPOSE 5050
ENTRYPOINT ["/opt/go-judge"]
