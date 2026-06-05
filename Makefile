.PHONY: help tidy fmt lint test cover

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

tidy: ## Sync go.mod / go.sum
	go mod tidy

fmt: ## Format the code
	gofmt -w .

lint: ## Run golangci-lint
	golangci-lint run ./...

test: ## Run tests with the race detector
	go test -race ./...

cover: ## Run tests and report coverage
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
