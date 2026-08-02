# Accessible Intake Gateway

Normalizes physical-site, hotline, and web legal-service intake envelopes into
one canonical request per applicant — with idempotency, source correlation,
consent handling (including revocation) and per-record validation errors.

## Source material

`materials/channel-contracts.json` is the canonical fixture and the runtime
registry. Channel IDs (`PHYSICAL`, `HOTLINE`, `WEB`), consent scopes,
accommodation codes and the minute-based `callbackWindowMinutes` field are
loaded from it and never renamed.

## Layout

- `cmd/server` — HTTP server entry point (standard library only).
- `internal/gateway` — contract registry, canonical model, SQLite store,
  normalization service, HTTP API, and all tests.
- `docs/api.md` — endpoint reference, envelope/result models, error codes,
  per-record transaction semantics.

## Delivery contract

- Go 1.24, standard-library HTTP stack, SQLite via `modernc.org/sqlite`
  (pure Go, no cgo). No web framework, no message broker.
- One canonical request model fed by the three channel adapters. Identity
  resolution is two-stage and deterministic: exact person key first, then
  scored candidate matching over normalized identity fragments
  (idNumber/phone/email/name), with strong-identifier conflicts blocking
  merges. Every accepted result records its match decision, rationale and
  hashed confidence evidence — never raw contact values before consent.
- Idempotency: same `eventId` + same payload replays the stored original
  result; same `eventId` + different payload is an explicit 409 conflict.
- Hotline callback windows stay minute-based and are classified
  deterministically: `0` → `IMMEDIATE`, `1..1440` → `SAME_DAY`,
  `>1440` → `CROSS_DAY`, negative/fractional → validation error.
- Native verification: `go test ./...` and `go run ./cmd/server`.

## Build, test, run

```sh
go test ./...        # unit + concurrency + end-to-end (httptest) suites
go run ./cmd/server  # serve on :8080 with ./intake.db
```

Server flags (env vars in parentheses):

| Flag                       | Default                                                       | Purpose                    |
| -------------------------- | ------------------------------------------------------------- | -------------------------- |
| `-addr` (`ADDR`)           | `:8080`                                                       | Listen address.            |
| `-db` (`DB_DSN`)           | `file:intake.db?_pragma=busy_timeout(5000)&_txlock=immediate` | SQLite DSN.                |
| `-contracts` (`CONTRACTS`) | `materials/channel-contracts.json`                            | Channel contract registry. |

Smoke check:

```sh
curl localhost:8080/healthz
curl -X POST localhost:8080/v1/intake/batches -d '{"records":[
  {"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","accommodations":["BRAILLE_MATERIAL"]},
  {"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"]},
  {"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]},
  {"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}
]}'
```

## Persistence setup

SQLite, schema versioned in `schema_migrations`. `Migrate` runs on startup
and applies pending migrations in version order — all DDL uses
`IF NOT EXISTS`, so a fresh database builds completely and re-running
migrations against an existing one is a no-op (covered by
`TestMigrateIsRepeatable` and `TestMigrationBackfillsIdentities`). Tables:

- `canonical_requests` — one row per applicant request; `person_key UNIQUE`
  is the guard that keeps concurrent cross-channel reports on a single chain.
- `events` — the accepted event chain (`seq` order), raw canonical payload,
  payload hash, and the stored original result (including match evidence)
  for byte-identical replay.
- `attempts` — every submission attempt with outcome and error detail, so
  failures, conflicts and replays are auditable even when nothing was applied.
- `request_identities` (migration v2) — normalized identity fragments per
  request powering deterministic candidate matching; v1 databases are
  backfilled automatically.

Writes are serialized through a single connection with `busy_timeout` and
immediate transactions; candidate matching executes inside the record's write
transaction, so concurrent claims always evaluate the same committed state.
Uniqueness constraints (not connection pooling) guarantee correctness.

## Retry behavior

- Same key + same payload → original result, `idempotentReplay: true`,
  attempt logged as `REPLAYED`. State is never re-folded.
- Same key + different payload → 409 `PAYLOAD_CONFLICT`, nothing applied.
- Validation failure → 422 with every field error; the event key is **not**
  consumed, so a corrected retry is processed on its own merits.
- Transient failure (adapter/DB) → full rollback, `retryable: true`
  (503 single / `FAILED` batch item), attempt logged as `TRANSIENT_FAILURE`.
  Recovery retries re-run deterministic matching against committed state and
  converge to the same single chain — never a fork.
- Out-of-order revocation → `REVOKES_UNKNOWN_EVENT`, retryable; resend after
  the grant lands. Retrying an old grant after revocation replays the original
  result and can never resurrect the revoked scope.
- Callback events may arrive before the web record exists: they anchor the
  request, and the later web event merges onto the same chain via identity
  evidence (see `docs/api.md` → Match evidence).

## Data-retention decisions

- Raw envelopes, results and attempts are **retained indefinitely** for audit
  replay; revocation changes the effective projection, never the history.
  A production deployment would add a scheduled purge aligned with the
  legal-services retention policy.
- Applicant PII is stored once per request (`person_json`). Projections expose
  only the name; phone/email appear solely while `CONTACT_CALLBACK` consent
  is effective, and `idNumber` is never exposed over the API.
- `stateHash` in the audit summary pins the folded state so minimal audit
  summaries, every attempt, and the final normalized result are all stably
  replayable.

Docker is not an acceptance requirement.
