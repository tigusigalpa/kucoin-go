# Contributing

1. Fork the repository and create a branch off `main`.
2. Add or update tests for any behavior change (`go test ./...` must pass).
3. Run `make check` (fmt + vet + lint + test) before opening a PR.
4. Every exported method must include a `Docs:` line in its docblock
   linking to the exact KuCoin API documentation page it implements.
5. Add or update the corresponding row in
   [internal/endpoints.yaml](internal/endpoints.yaml), then run
   `go run ./internal/gendocs` to regenerate
   [docs/ENDPOINTS.md](docs/ENDPOINTS.md). CI fails if the generated file
   doesn't match what's committed.
6. Never add an example or test that places a live order, transfer, or
   withdrawal by default — see [SECURITY.md](SECURITY.md).
7. Open a pull request describing the change and the doc URL(s) it covers.

## Roadmap / phases

This project follows the phased scope described in the root
[README.md](README.md#status). Please check open issues before starting
work on a new domain, so effort isn't duplicated.

Found a security issue? See [SECURITY.md](SECURITY.md) instead of opening
a public issue.
