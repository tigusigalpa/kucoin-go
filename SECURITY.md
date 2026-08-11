# Security Policy

## Reporting a Vulnerability

Email **sovletig@gmail.com** directly. Do not open a public issue for
security vulnerabilities.

Include: affected version, a minimal reproduction, and the potential
impact. Expect an initial response within a few days.

## Handling API credentials

This library never logs `KC-API-KEY`, `KC-API-SIGN`, `KC-API-PASSPHRASE`,
or full private request bodies (see `transport.Logger`). It never makes
trading, transfer, or withdrawal calls in its own tests, examples, or CI.

You are responsible for:

- Restricting your KuCoin API key to the minimum permissions it needs
- Enabling IP whitelisting on the key where possible
- Never committing credentials — use environment variables or a secret store
- Reviewing any example before running it against a real account
