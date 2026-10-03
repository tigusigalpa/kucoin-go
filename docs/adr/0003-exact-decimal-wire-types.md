# ADR-0003: Exact decimal wire types for streamed and market-data payloads

- Date: 2026-10-03
- Status: accepted

## Context

The SDK models money as `string` ("no float rounding errors"). Verified against
the live API, KuCoin's wire format is not that consistent:

- the same logical field is a quoted string in one feed and a bare JSON number in
  another (Futures REST order-book levels are `[84491.3,1220]`, UTA call-auction
  prices are numbers while the schema says strings);
- rates use Java exponent notation (`1.0E-4`, `-5.36E-4`, `6.845151044888E8`) and
  equal prices come with different trailing zeros (`84486.0` in REST, `"84486"` on
  the WebSocket), so string equality is wrong for order-book levels;
- identifiers and timestamps switch between number and string (`ti`, `time`,
  `version`), and empty strings (`""`) appear for absent values;
- some values need more than 17 significant digits
  (`turnover: 1033552780.2532196044`), so `float64` silently corrupts them, and a
  plain `string` field fails to decode a bare number at all.

## Options

1. `float64` — loses digits; rejected.
2. `string` fields — fails on bare numbers and exponent forms.
3. `json.Number` — rejects the empty string KuCoin sends for "no value".
4. A third-party decimal library — adds a dependency and its own wire behaviour.
5. A small in-module type that decodes strings *and* numbers and keeps the exact
   text.

## Decision

Option 5, in package `types`:

- `Decimal` (underlying `string`): accepts a JSON string, a JSON number (including
  exponents) or `null`; keeps the literal; marshals as a string. Helpers:
  `Canonical` (plain notation, no trailing zeros — the safe map key), `Cmp`,
  `Sign`, `IsZero`, `Rat`, `Float64`, `Int64`, exact `Add`/`Sub`/`Mul`, and the
  allocation-free `CompareCanonical` used by the order book.
- `ID`: number-or-string identifiers kept verbatim (no 64-bit rounding).
- `Int64`: counts and timestamps that are numbers in the documentation and
  occasionally numeric strings on the wire.

All new typed payloads and the Futures REST models use them. Existing REST models
keep their `string` fields (changing them would be a breaking change with no
benefit for the endpoints already verified to return strings).

## Consequences

- Consumers never parse raw JSON and never lose a digit; arithmetic is explicit
  (`Rat`, `Add`, ...), comparison is numeric (`Cmp`), not textual.
- The order book is keyed by canonical prices, so REST and WebSocket spellings of
  one price are one level.
- Two representations of "a number as text" coexist in the module (`string` in the
  older REST packages, `types.Decimal` in the new ones); `Decimal` converts
  trivially with `string(d)`/`types.Decimal(s)`.

## Verification

`types` tests cover every accepted spelling, null handling, canonicalisation,
comparison against `math/big` on 20 000 random pairs, and exact arithmetic against
`big.Rat` on 5 000 random operations; payload tests decode the official examples and
the live samples and assert the exact decimal text.
