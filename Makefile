.PHONY: test test-verbose test-coverage fmt lint tidy build docs check help

test:
	go test ./...

test-verbose:
	go test -v ./...

test-coverage:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

fmt:
	gofmt -l -s -w .

lint:
	golangci-lint run --timeout=5m

tidy:
	go mod tidy

build:
	go build ./...

docs:
	go run ./internal/gendocs

check: fmt lint test docs
	git diff --exit-code -- docs/ENDPOINTS.md

help:
	@echo "make test           run the test suite"
	@echo "make test-coverage  run tests with coverage"
	@echo "make fmt            gofmt all files"
	@echo "make lint           run golangci-lint"
	@echo "make tidy           go mod tidy"
	@echo "make build          build all packages"
	@echo "make docs           regenerate docs/ENDPOINTS.md from the manifest"
	@echo "make check          fmt + lint + test + docs-drift check"
