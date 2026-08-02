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
- `erased_identity_evidence` (migration v3, with
  `canonical_requests.contact_erased`) — non-reversible hashes of contact
  fragments purged by revocation; keeps matching convergent without
  retaining raw contact.

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
  result and can never resurrect the revoked scope — or erased contact data.
- Callback events may arrive before the web record exists: they anchor the
  request, and the later web event merges onto the same chain via identity
  evidence (see `docs/api.md` → Match evidence).

## Contact erasure on revocation

Consent folding and contact storage are separate concerns. When a revocation
removes the **last live `CONTACT_CALLBACK` grant**, the same transaction
purges raw contact data: `person_json` loses phone/email, live contact
identity fragments move to `erased_identity_evidence` (hash + erasing event +
timestamp), and stored payload copies on the chain are redacted to
`REDACTED#<fragmentHash>` markers. `payload_hash` columns stay original, so
replay integrity is unchanged.

- Only the revoked scope's data is erased; other scopes, accommodations,
  names, idNumbers and callback windows are never over-cleared, and erasure
  never crosses chains.
- Repeated revocation is idempotent: replay returns the stored result, and a
  second distinct revocation of an erased scope is an accepted no-op.
- A late retry carrying the revoked phone cannot resurrect it: without a
  fresh `CONTACT_CALLBACK` grant, contact fields/fragments are not
  re-registered and the new payload copy is stored redacted. Matching still
  converges to the same chain via the non-reversible erased-fragment hashes
  (`"redacted": true` evidence).
- A fresh `CONTACT_CALLBACK` grant on a later event is a new consent and
  re-enables contact registration from that event onward.

## Data-retention decisions

- Event evidence is **retained indefinitely** for audit replay — but raw
  contact data is consent-gated at rest: while `CONTACT_CALLBACK` is
  effective the chain keeps raw payloads; once its last grant is revoked,
  raw phone/email are purged from person records, identity fragments and
  stored payload copies, leaving only non-reversible hashes and original
  `payload_hash` values. A production deployment would add a scheduled purge
  aligned with the legal-services retention policy for the remaining data.
- Applicant PII is minimized at every boundary: projections expose only the
  name; phone/email appear solely while `CONTACT_CALLBACK` consent is
  effective; `idNumber` is never exposed; match and erasure evidence carry
  fragment hashes only.
- `stateHash` in the audit summary pins the folded state so minimal audit
  summaries, every attempt, and the final normalized result are all stably
  replayable.

Docker is not an acceptance requirement.
