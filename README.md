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

---

## Implementation overview

The gateway normalizes three channel adapters — `PHYSICAL` (deskReceiptNo /
visitor), `HOTLINE` (callRef / caller / callbackWindowMinutes), and `WEB`
(submissionId / applicant) — into one canonical request model. It is written
in Go 1.24 using only `net/http` for the API and SQLite (via the pure-Go
`modernc.org/sqlite` driver) for persistence. There is no web framework and no
message broker.

```
cmd/server                 main(): load contracts, open SQLite, serve HTTP
internal/contracts         embedded fixture, code/scope/channel validation
internal/canonical         normalized envelope, per-item validator, payload hash
internal/store             SQLite schema, idempotent migrations, replay
internal/intake            service: idempotency, conflict, revocation, merge
internal/httpapi           net/http handlers and router
tests/e2e_test.go          end-to-end scenarios over httptest
```

## Running and verifying

```bash
# Full test suite (unit + end-to-end, including -race)
GOTOOLCHAIN=go1.24.0 go test ./...
GOTOOLCHAIN=go1.24.0 go test -race ./...

# Start the server (schema is created/migrated automatically on boot)
GOTOOLCHAIN=go1.24.0 go run ./cmd/server
#   INTAKE_DB    SQLite path      (default intake.db)
#   INTAKE_ADDR  listen address   (default :8080)

curl -s http://localhost:8080/healthz
```

The Go directive in `go.mod` is `go 1.24`; with `GOTOOLCHAIN=auto` (the default)
the toolchain is fetched automatically if 1.24 is not installed.

## Persistence setup and repeatable rebuilds

SQLite is opened with WAL journaling, foreign keys on, a 5s busy timeout, and
`_txlock=immediate` so every write transaction is `BEGIN IMMEDIATE`. The schema
in `internal/store/store.go` runs on every startup with `CREATE TABLE IF NOT
EXISTS`, and later column additions are applied through idempotent `ALTER TABLE`
statements that ignore "duplicate column" errors. Deleting the database file and
restarting therefore rebuilds an identical schema from scratch.

Tables:

| Table | Purpose |
|---|---|
| `canonical_requests` | One row per merged applicant chain (subject key, current person/contact snapshot, sequence counter). |
| `events` | One row per committed event: channel, source correlation id, payload hash, normalized payload, sequence, per-item results. |
| `event_attempts` | One row per submission attempt (OK / CONFLICT / VALIDATION_ERROR / ADAPTER_TRANSIENT) for stable replay of every try. |
| `consent_records` | Append-only GRANT/REVOKE entries per scope; the latest entry wins. |
| `accommodation_records` | Valid accommodation codes requested on the chain (deduped). |
| `validation_errors` | Per-item error rows tied to an event and attempt. |
| `schema_migrations` | Applied migration versions. |

## API

All request and response bodies are `application/json`. The canonical payload
hash is computed over a key-sorted JSON serialization, so field reordering does
not change the hash but any value change does.

### `POST /v1/intake/events`

Accepts an event from any channel using that channel's native field names (a
normalized `person` alias is also accepted on every channel).

Physical example:
```bash
curl -s -X POST localhost:8080/v1/intake/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId":"EV-P-001","channel":"PHYSICAL",
    "deskReceiptNo":"D-88",
    "visitor":{"fullName":"Jane Doe","phone":"+1-555-0100","email":"jane@example.com"},
    "accommodations":["BRAILLE_MATERIAL"]
  }'
```

Hotline example:
```bash
curl -s -X POST localhost:8080/v1/intake/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId":"EV-H-001","channel":"HOTLINE",
    "callRef":"C-19","caller":{"fullName":"Jane Doe","phone":"+1-555-0100"},
    "callbackWindowMinutes":45,
    "consent":["CONTACT_CALLBACK"]
  }'
```

Web example (and revocation):
```bash
curl -s -X POST localhost:8080/v1/intake/events -H 'Content-Type: application/json' -d '{
  "eventId":"EV-W-001","channel":"WEB","submissionId":"W-71",
  "applicant":{"fullName":"Jane Doe","email":"jane@example.com"},
  "consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]}'

curl -s -X POST localhost:8080/v1/intake/events -H 'Content-Type: application/json' -d '{
  "eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}'
```

Response `200 OK`:
```json
{
  "eventId": "EV-P-001",
  "status": "ACCEPTED",
  "canonicalRequestId": "CR-...",
  "idempotentReplay": false,
  "itemResults": [],
  "canonical": { "id":"CR-...", "accommodations":["BRAILLE_MATERIAL"], "consent":[...], "sources":[...], "contactMasked": false }
}
```

Other endpoints:

| Method / path | Description |
|---|---|
| `GET /healthz` | Liveness probe. |
| `GET /v1/intake/events/{eventId}` | Committed event and its per-item results. |
| `GET /v1/intake/events/{eventId}/attempts` | Every attempt for an event (oldest first). |
| `GET /v1/intake/canonical/{id}` | Current masked canonical snapshot. |
| `GET /v1/intake/canonical/{id}/audit` | Minimal replayable audit summary. |
| `POST /v1/intake/canonical/{id}/replay` | Reconstructs the snapshot from the event log and returns it with the events applied. |

## Idempotency, conflicts, and retry semantics

- **Idempotency key** is `eventId`. The canonical `payloadHash` is stored on
  first commit.
- **Same `eventId` + same payload hash** returns the original outcome with
  `idempotentReplay: true` and HTTP 200. An additional attempt row is recorded
  so retries are visible in the audit trail.
- **Same `eventId` + different payload hash** returns HTTP `409 Conflict` with a
  body containing both hashes. No event or domain data is mutated; only an
  attempt row is recorded. This is the "same key with changed payload must
  conflict" guarantee.
- **Adapter transient failure**: an event submitted with
  `"simulateAdapterTransient": true` records an `ADAPTER_TRANSIENT` attempt and
  returns HTTP `503` *without* committing the event. The next submission of the
  same `eventId` commits once and only once; the attempt trail shows the 503
  followed by the OK. This models a channel adapter timing out before the
  gateway could answer, and the safe retry that follows.
- **Out-of-order revocation**: a revocation whose `revokes` target does not yet
  exist returns HTTP `400` with `REVOCATION_TARGET_MISSING`. The adapter should
  retry it after the target event lands.

## Per-item transaction semantics

Validation is split into fatal and per-item failures:

- **Fatal** (missing `eventId`, unknown channel on a non-revocation event,
  revocation without a target, no person identifier at all) reject the whole
  submission: no event is committed, only an attempt + validation errors are
  recorded, and HTTP 400 is returned.
- **Per-item** errors do not abort the event. Each item is validated
  independently and valid items are committed while invalid items appear in
  `itemResults`:
  - unknown accommodation code → `UNKNOWN_CODE`, that code is not stored;
  - unknown consent scope → `UNKNOWN_SCOPE`, that scope is not granted;
  - negative `callbackWindowMinutes` → `INVALID_VALUE`, field is ignored;
  - missing native source id → `MISSING_FIELD`; event still accepted.
  The event itself is `ACCEPTED` with HTTP 200 and `itemResults` enumerating
  exactly which items succeeded (`OK`) and which failed.

This means one event can partially apply: a bad accommodation code never blocks
a valid consent grant or callback window in the same submission.

## Consent, revocation, and contact masking

- `CONTACT_CALLBACK`, `CASE_SUMMARY_TRANSFER`, and `ACCOMMODATION_TRANSFER` are
  the only valid scopes. Each event grants the valid scopes it lists; a
  revocation event writes `REVOKE` records for the scopes named in `scopes`.
- The effective consent set is rebuilt from the append-only
  `consent_records` log: the latest action per scope wins.
- **Contact details (phone, email) are projected into any response only when
  `CONTACT_CALLBACK` is currently granted.** When it is absent or has been
  revoked, `contactMasked: true` is returned and `phone`/`email` are empty.
- **A retry can never re-expose withdrawn contact data.** An idempotent retry
  returns the original event outcome but recomputes the canonical snapshot from
  current state, so a later revocation remains in force regardless of how many
  times the original event is retried.

## Concurrency: one event chain per applicant

Applicants are linked into one canonical chain when they share any stored
identifier — `referenceNumber`, normalized phone digits, or email — looked up
inside the write transaction. Because every write uses `BEGIN IMMEDIATE` and the
pool is limited to one connection, two channels reporting the same applicant at
the same instant serialize; the second transaction finds the chain created by
the first and appends to it. The result is exactly one canonical request with
contiguous sequence numbers, never two duplicated chains. Two concurrent
submissions of the *same* `eventId` likewise commit exactly one event row; the
other receives the idempotent replay.

## Audit and stable replay

`GET /canonical/{id}/audit` returns a minimal, deterministic summary: counts of
events and attempts, the current consent and accommodation sets, source
correlation handles, and every committed event in sequence order.
`POST /canonical/{id}/replay` reconstructs the canonical snapshot purely from
the `events`, `consent_records`, and `accommodation_records` log (sorted by
sequence/id); reconstruction is a pure function of those rows, so the same
database always replays to the same result. Every attempt is retained in
`event_attempts`, including transient failures and conflicts, so the full
submission history is replayable.

## Data-retention decisions

- The database is file-based and append-oriented: events, attempts, consent
  records, and validation errors are never auto-deleted. This is deliberate so
  that audit and replay remain stable.
- Revocation does not delete data; it adds a `REVOKE` record and suppresses
  projection of contact fields. The raw values remain on the server for legal
  audit but are not exposed through the API while revocation is in force.
- To purge or archive data, operators rotate the SQLite file (the schema
  rebuilds idempotently on a fresh file). There is no TTL job in the service;
  retention policy should be applied at the storage/backup layer per
  jurisdiction.


