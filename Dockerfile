# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/aethercode-exec ./cmd/stj-exec

# ---- Runtime stage ----
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/aethercode-exec /usr/local/bin/aethercode-exec
# 5100 = HTTP gateway + SSE, 6060 = Prometheus metrics + pprof
EXPOSE 5100 6060
ENTRYPOINT ["aethercode-exec"]
