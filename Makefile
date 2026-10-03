.PHONY: test race test-verbose test-coverage fmt vet lint tidy build docs check help

test:
	go test ./...

# The race detector needs cgo (a C compiler); CI always runs it.
race:
	go test -race ./...

test-verbose:
	go test -v ./...

test-coverage:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

fmt:
	gofmt -l -s -w .

vet:
	go vet ./...

lint:
	golangci-lint run --timeout=5m

tidy:
	go mod tidy

build:
	go build ./...

docs:
	go run ./internal/gendocs

check: fmt vet lint test docs
	git diff --exit-code -- docs/ENDPOINTS.md docs/CHANNELS.md

help:
	@echo "make test           run the test suite"
	@echo "make race           run the test suite with the race detector (needs cgo)"
	@echo "make test-coverage  run tests with coverage"
	@echo "make fmt            gofmt all files"
	@echo "make vet            go vet (includes the too-new-stdlib check)"
	@echo "make lint           run golangci-lint"
	@echo "make tidy           go mod tidy"
	@echo "make build          build all packages"
	@echo "make docs           regenerate docs/ENDPOINTS.md and docs/CHANNELS.md from the manifests"
	@echo "make check          fmt + vet + lint + test + docs-drift check"
