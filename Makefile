BINARY  := aethercode-exec
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GOFLAGS ?= -trimpath
LDFLAGS ?= -s -w -X github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/gateway.Version=$(VERSION)

.PHONY: help build test vet lint fmt e2e loadgen up down clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the engine binary
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/stj-exec

test: ## Run unit tests
	go test ./...

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format all Go source
	gofmt -s -w .

e2e: ## Run the e2e suite (requires a running stack on :5100)
	bash scripts/test_e2e.sh

loadgen: ## Build the load generator to /tmp/loadgen
	go build -o /tmp/loadgen ./cmd/loadgen

up: ## Build and start the full stack (docker compose)
	docker compose up --build

down: ## Stop the full stack
	docker compose down

clean: ## Remove build artifacts
	rm -f $(BINARY) /tmp/loadgen
