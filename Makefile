.PHONY: help dev up down logs build test test-integration migrate-down lint fmt tidy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

dev: up ## Start Postgres, Redis, API and scheduler, then tail their logs
	@echo "Control plane is up. Start a runner with: go run ./cmd/runner"
	docker compose logs -f api scheduler

up: ## Start the control plane in the background
	docker compose up -d --build

down: ## Stop everything and remove volumes
	docker compose down -v

logs: ## Tail all container logs
	docker compose logs -f

build: ## Build all three binaries into ./bin
	go build -o bin/ ./cmd/...

test: ## Run unit tests
	go test ./...

test-integration: ## Run the end-to-end test (needs `make up` first)
	FORGERUN_INTEGRATION=1 go test -v -count=1 ./tests/...

migrate-down: ## Undo the last migration (STEPS=1 by default). Drops data.
	go run ./cmd/api --rollback=$(or $(STEPS),1)

lint: ## Vet and check formatting
	go vet ./...
	@test -z "$$(gofmt -l cmd internal tests)" || (echo "unformatted files:"; gofmt -l cmd internal tests; exit 1)

fmt: ## Format the code
	gofmt -w cmd internal tests

tidy: ## Tidy go.mod
	go mod tidy

clean: ## Remove build output
	rm -rf bin
