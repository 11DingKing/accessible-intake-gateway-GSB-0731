# Accessible Intake Gateway

Blank 0-1 baseline for normalizing physical-site, hotline, and web legal-service intake envelopes. No application code or hidden solution is included.

## Source material

Use `materials/channel-contracts.json` as the canonical fixture. Do not silently rename its channel IDs, consent scopes, accommodation codes, or minute-based callback field.

## Required delivery contract

- Go 1.24, standard-library HTTP stack, and SQLite; no web framework or message broker.
- Channel adapters feed one canonical request model with idempotency, source correlation, consent handling, and per-record validation errors.
- Native verification: `go test ./...` and `go run ./cmd/server`.
- Document endpoint examples, persistence setup, retry behavior, and data-retention decisions.

Docker is not an acceptance requirement.

