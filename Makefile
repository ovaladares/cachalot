all: help

.PHONY: all test clean

## Test:
test: ## Run all tests
	go clean -testcache
	go test -race -timeout 60s ./...

coverage: ## Run the tests of the project and export the coverage
	go clean -testcache
	go test -timeout 30s -cover -covermode=count -coverprofile=profile.cov ./...
	go tool cover -html profile.cov