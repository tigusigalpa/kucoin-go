# ADR-0001: Add current UTA v2 services without replacing the v1 API

- Date: 2026-10-03
- Status: accepted

## Context

KuCoin's current documentation places the existing UTA REST v1 endpoint
families in its abandoned section while presenting UTA REST v2 as the active
surface. Existing users already import `Client.UTA.Market`, `Account`,
`Orders`, `Positions`, and `Leverage`; replacing those types or paths would be
a breaking change and would obscure protocol-level differences.

## Options

1. Replace the existing UTA v1 services with v2 methods and types.
2. Leave the SDK v1-only and document current UTA endpoints as unavailable.
3. Keep v1 services for source compatibility and add an explicitly separate
   v2 namespace.

## Decision

Choose option 3. Current UTA services are added under `Client.UTA.V2`,
starting with the complete current Market Data group at
`Client.UTA.V2.Market`. The pre-existing v1 services remain available but are
documented as compatibility APIs, not counted as current v2 coverage.

## Consequences

- Existing callers keep compiling without a migration.
- Current and legacy response models cannot accidentally be presented as one
  interchangeable type hierarchy.
- The temporary duplication is intentional and requires method-level docs and
  tests for each new v2 service.
- A future major version may retire the legacy root only with an explicit
  migration guide; this ADR does not authorize that breaking change.

## Verification

- `uta/v2/market` has mock HTTP tests for all 23 documented current Market
  Data methods.
- [API_COVERAGE.md](../API_COVERAGE.md) distinguishes current v2 from retained
  v1 coverage.
- `go test ./...`, `go test -race ./...`, and `go vet ./...` are the required
  repository checks.
