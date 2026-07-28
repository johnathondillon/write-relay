.PHONY: help fmt fmt-check verify test race vet vuln lint build check postgres-up postgres-down setup integration

help:
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format Go source
	gofmt -w $$(rg --files -g '*.go' -g '!vendor/**' -g '!.gomodcache/**')

fmt-check: ## Verify Go formatting without changing files
	@test -z "$$(gofmt -l $$(rg --files -g '*.go' -g '!vendor/**' -g '!.gomodcache/**'))"

verify: ## Verify downloaded modules against go.sum
	go mod verify

test: ## Run unit tests
	go test ./...

race: ## Run unit tests with the race detector
	go test -race ./...

vet: ## Run go vet
	go vet ./...

vuln: ## Scan reachable code with the pinned Go vulnerability scanner
	go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

lint: ## Run golangci-lint when installed
	@command -v golangci-lint >/dev/null || { echo "golangci-lint is not installed"; exit 1; }
	golangci-lint run

build: ## Build all Go packages
	go build ./...

check: fmt-check verify build test vet ## Run local required checks

postgres-up: ## Start development PostgreSQL
	docker compose up -d --wait postgres

postgres-down: ## Stop development PostgreSQL
	docker compose down

setup: ## Install SQL objects and create the development slot
	go run ./cmd/writerelayd setup --config ./writerelay.yaml --create-slot

integration: postgres-up ## Run Docker-backed integration tests
	go test -tags=integration -count=1 -v ./tests/integration/...
