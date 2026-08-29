# kucoin-go v1.1.13

## Fixed

- Fixed REST request construction when a caller supplies query parameters both
  in the endpoint path and through the `query` argument. Additional
  parameters are now appended with `&` instead of a second `?`, so KuCoin
  receives every parameter correctly.

## Validation

- Added regression coverage for preserving path query parameters while
  appending SDK-provided query values.
- Verified with `go test ./transport` and `go vet ./transport`.
