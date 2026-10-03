# Contributing

1. Fork the repository and create a branch off `main`.
2. Add or update tests for any behavior change. `go test ./...` and
   `go test -race ./...` must pass, as must `go vet ./...` (which also fails the
   build when a standard-library symbol newer than the Go version in `go.mod` is
   used) and `gofmt -l .` (which must print nothing).
3. Run `make check` (fmt + vet + lint + test + docs drift) before opening a PR, and
   `make race` if you have a C compiler (CI always runs the race detector).
4. Every exported method must include a `Docs:` line in its docblock linking to
   the exact KuCoin API documentation page it implements.
5. **REST endpoint:** add or update the row in
   [internal/endpoints.yaml](internal/endpoints.yaml). Every exported method of a
   tracked service (see `restServices` in [manifest_test.go](manifest_test.go))
   needs a row, and its `test:` entry must name a test that exists.
6. **WebSocket channel:** add or update the row in
   [internal/channels.yaml](internal/channels.yaml): the session group, the typed
   `Subscribe…` method, the topic, the payload type, the test and the KuCoin docs
   page. A channel needs a typed payload struct (no `json.RawMessage` in a public
   payload), a decoder test that uses the worked example of KuCoin's own
   documentation (the fixtures of an existing streaming package show the pattern),
   and a test that a malformed frame is reported as a decode error without ending
   the subscription. Where the documentation and the live service disagree, follow
   the live service and say so in the field's comment.
7. After editing a manifest run `go run ./internal/gendocs` to regenerate
   [docs/ENDPOINTS.md](docs/ENDPOINTS.md) and [docs/CHANNELS.md](docs/CHANNELS.md).
   CI fails if the generated files do not match what is committed, and the root
   package's manifest tests fail if a row does not resolve to a real method or a
   `Subscribe…` method has no row.
8. Concurrency changes (anything under `internal/wsengine`, `stream`, `orderbook`
   or a streaming package) need a test under `-race` and must keep the contracts
   listed in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): one owner per channel, no
   lock held across user code or a blocking send, no goroutine left behind
   (`wstest.CheckLeaks`).
9. Never add an example or test that places a live order, transfer, or
   withdrawal by default — see [SECURITY.md](SECURITY.md). Tests and examples
   that touch the live service use public, read-only endpoints only and are never
   part of `go test ./...`.
10. Open a pull request describing the change and the doc URL(s) it covers, and
    add an entry under `Unreleased` in [CHANGELOG.md](CHANGELOG.md).

## Roadmap / phases

This project follows the phased scope described in the root
[README.md](README.md#status). Please check open issues before starting
work on a new domain, so effort isn't duplicated.

Found a security issue? See [SECURITY.md](SECURITY.md) instead of opening
a public issue.
