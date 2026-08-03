# Accessible Intake Gateway

A Go 1.24, standard-library HTTP service that normalizes legal-service intake
events from three channels — **physical site (`PHYSICAL`)**, **hotline
(`HOTLINE`)**, and **web (`WEB`)** — into a single *canonical request* per
applicant. It preserves source correlation IDs, provides event idempotency,
tracks reasonable-accommodation codes and consent scopes, records per-item
validation results, and produces a replayable audit trail.

No web framework and no message broker are used. Persistence is SQLite via the
pure-Go `modernc.org/sqlite` driver (no CGO required).

## Source material

`materials/channel-contracts.json` is the canonical fixture. Its channel IDs
(`PHYSICAL`, `HOTLINE`, `WEB`), per-channel source field names (`deskReceiptNo`,
`callRef`, `submissionId`), person field names (`visitor`, `caller`,
`applicant`), the minute-based `callbackWindowMinutes`, accommodation codes and
consent scopes are loaded at startup and never silently renamed.

## Layout

```
cmd/server/                 HTTP entry point
internal/contracts/         loads and validates channel-contracts.json
internal/intake/            envelope decoding, per-item validation, hashing,
                            deterministic canonical projection
internal/store/             SQLite schema, idempotent transactions, retry, audit
internal/adapter/           downstream adapter interface + deterministic recorder
internal/api/               net/http handlers and routing
materials/channel-contracts.json
```

## Run

```bash
go test ./...
go run ./cmd/server -addr :8080 -db intake.db -contracts materials/channel-contracts.json
```

The database schema is created automatically on every startup (idempotent
`CREATE TABLE IF NOT EXISTS`), so deleting `intake.db` rebuilds a clean,
repeatable database. SQLite runs in WAL mode with `foreign_keys=ON` and a
single writer connection plus a `busy_timeout`.

Flags:

| Flag          | Default                            | Purpose                                   |
|---------------|------------------------------------|-------------------------------------------|
| `-addr`       | `:8080`                            | HTTP listen address                       |
| `-db`         | `intake.db`                        | SQLite path                               |
| `-contracts`  | `materials/channel-contracts.json` | Channel/source/accommodation/consent data |

Health check: `GET /healthz`.

## API

All request and response bodies are JSON.

### `POST /api/v1/events` — submit a normalized channel event

The request uses the **channel-native field names** from the contract so that
source envelopes are preserved verbatim. Cross-channel identity is derived from
`firstName`, `lastName` and `dateOfBirth`.

Physical example:

```json
{
  "eventId": "EV-P-001",
  "channel": "PHYSICAL",
  "deskReceiptNo": "D-88",
  "visitor": {
    "firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10",
    "email": "ada@example.com", "phone": "555-0100"
  },
  "accommodations": ["BRAILLE_MATERIAL"]
}
```

Hotline example (note the minute-based `callbackWindowMinutes`):

```json
{
  "eventId": "EV-H-001",
  "channel": "HOTLINE",
  "callRef": "C-19",
  "caller": {"firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10"},
  "callbackWindowMinutes": 45,
  "consent": ["CONTACT_CALLBACK"]
}
```

Web example:

```json
{
  "eventId": "EV-W-001",
  "channel": "WEB",
  "submissionId": "W-71",
  "applicant": {"firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10"},
  "consent": ["CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"],
  "summary": "Housing intake"
}
```

Revocation example (same shape as `revocation` in the fixture):

```json
{
  "eventId": "EV-W-002",
  "channel": "WEB",
  "submissionId": "W-72",
  "applicant": {"firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10"},
  "revokes": "EV-W-001",
  "revocationScopes": ["CASE_SUMMARY_TRANSFER"]
}
```

Optional field `canonicalRequestId` may be supplied to force an event into an
existing chain; otherwise the server correlates by applicant identity.

Response `200 OK`:

```json
{
  "result": {
    "eventId": "EV-P-001",
    "canonicalRequestId": "CR-...",
    "channel": "PHYSICAL",
    "sourceId": "D-88",
    "status": "accepted",
    "itemResults": [{"field": "person", "status": "applied"}, ...],
    "payloadHash": "sha256-hex...",
    "sequence": 1,
    "deliveryStatus": "delivered",
    "attempts": [{"attemptNo": 1, "outcome": "accepted", "at": "..."}],
    "createdAt": "..."
  },
  "canonical": { "requestId": "CR-...", "eventChain": ["EV-P-001"], ... }
}
```

| HTTP | Meaning |
|------|---------|
| `200 OK` | Event accepted and delivered; or a duplicate of an already-accepted event. |
| `202 Accepted` | Event durably accepted but the downstream adapter failed transiently; `deliveryStatus` is `pending` and the event may be retried. |
| `400 Bad Request` | Structural error (invalid JSON, missing `eventId`/`channel`/source ID or unknown channel). `itemResults` lists the hard errors. |
| `409 Conflict` | The same `eventId` already exists with a **different** payload hash. |

### `GET /api/v1/events/{eventId}` — fetch one event result (with all attempts)

### `POST /api/v1/events/{eventId}/retry` — retry a pending downstream delivery

Recomputes the canonical projection **at retry time**, so a later revocation is
already reflected. Returns the updated event result and projection.

### `GET /api/v1/requests/{requestId}` — current canonical projection

Returns the normalized person (with phone/email removed when
`CONTACT_CALLBACK` is not currently granted), accommodations (empty unless
`ACCOMMODATION_TRANSFER` is granted), summary (empty unless
`CASE_SUMMARY_TRANSFER` is granted), per-channel source references, callback
window, event chain and a stable `projectionHash`.

### `GET /api/v1/requests/{requestId}/events` — full event chain with per-item results

### `GET /api/v1/requests/{requestId}/audit` — minimum audit summary

```json
{
  "requestId": "CR-...",
  "eventCount": 3,
  "lastSequence": 3,
  "projectionHash": "...",
  "chain": ["EV-P-001", "EV-H-001", "EV-W-001"],
  "generatedAt": "..."
}
```

### `GET /api/v1/events/{eventId}/match` — identity-match rationale

Returns the deterministic match decision that attached an event to its
canonical request: `confidence` (`exact`/`high`/`medium`/`low`/`none`),
machine-readable `reason`, numeric `score`, a `conflict` flag, and per-field
`evidence`. Contact values in evidence are masked unless the field itself is a
confirmed match. Reasons include `EXACT_IDENTITY`, `PHONE_MATCH`,
`EMAIL_MATCH`, `NAME_DOB_PARTIAL_MATCH`, `CROSS_CHANNEL_LINK`,
`FRAGMENT_CONFLICT` and `NO_MATCH`.

### `GET /api/v1/requests?status=pending` — list fragment-only pending candidates

Lists canonical requests created from partial identity fragments (e.g. a
hotline callback that arrived before the web record) that have not yet been
confirmed by a complete identity.

## Hotline callback events and identity resolution

A hotline **callback event** is a `HOTLINE` channel event carrying
`callbackWindowMinutes`. It may arrive **before** the web/physical record
exists, and may carry only an identity **fragment** (for example just a phone
number) or a fragment that partly matches and partly conflicts with an existing
request (same name/phone, different DOB).

`callbackWindowMinutes` continues to use the minute unit from the first round.
It is validated item-by-item:

| Value  | Code / result                                                        |
|--------|---------------------------------------------------------------------|
| absent | no callback window contribution                                      |
| `0`    | applied as `IMMEDIATE_CALLBACK` (call immediately)                   |
| `< 0`  | rejected as `INVALID_CALLBACK_WINDOW`; window dropped                |
| `> 1440` | rejected as `CROSS_DAY_CALLBACK_WINDOW` (same-day window exceeded)  |
| `1..1440` | applied as a same-day callback window in minutes                   |

### Deterministic matching

Each event contributes an identity fragment. When a new event is submitted the
resolver looks, in order, at:

1. explicit `canonicalRequestId`;
2. exact identity (`firstName` + `lastName` + `dateOfBirth`) → `exact`;
3. strong-identifier match on normalized **phone** or **email** across stored
   fragments → `high`;
4. name/partial-DOB matches → `medium`;
5. no shared signal → a new `pending` candidate.

When multiple candidates match, the highest score wins, with the lowest
`requestId` as the deterministic tie-breaker. Conflicting fields (e.g. a
matching phone but a different DOB) are recorded as conflict evidence but do
**not** silently overwrite the canonical person; the first-established value for
each field wins in the projection, and the conflict is surfaced in `match` and
`GET .../match`.

If the strong-identifier match is `exact`/`high`, the event is attached to the
existing request automatically. When a later full-identity event arrives, all
matching pending candidates are consolidated into that single chain. Original
contact data is never exposed in projections until `CONTACT_CALLBACK` is
granted; evidence values for non-matching contacts are always masked.

### One chain under out-of-order, concurrent and failure scenarios

- **Out-of-order arrival**: a phone-only hotline callback first creates a
  `pending` request. The later web event matches by normalized phone and merges
  into that same request; only one `canonicalRequestId` and one event chain
  result.
- **Two channels concurrently claim the same applicant**: writes are serialized
  through one SQLite connection (WAL + `busy_timeout`); the second transaction
  sees the first's fragment and matches into the same request instead of
  creating another.
- **Adapter transient failure followed by recovery**: the failed event stays on
  its chain with `deliveryStatus=pending`; a subsequent same-person event and a
  later `/retry` both operate on that same chain, and the retried projection
  already contains every event (and any revocation) recorded so far. A retry can
  never re-expose a contact channel whose consent was withdrawn.

## Idempotency, conflicts and per-item transaction semantics

- **Idempotency key** = `eventId`. Each event is also fingerprinted with a
  deterministic `payloadHash` over its normalized fields.
- The submission path runs in **one SQLite transaction**:
  1. look up the event id; if it exists, compare payload hashes;
  2. **same key + same payload** → return the stored result and current
     projection (HTTP `200`); the adapter is **not** invoked again;
  3. **same key + different payload** → explicit conflict (HTTP `409`), no
     state is mutated;
  4. otherwise validate every field independently, insert the event and its
     `itemResults`, update the canonical request, call the downstream adapter,
     and append attempt #1 — all atomically.
- **Per-item validation**: each person field, accommodation code, consent scope,
  revocation scope and the numeric `callbackWindowMinutes` is checked
  individually. Valid items are applied; invalid items are reported in
  `itemResults` with a machine-readable code (`UNKNOWN_ACCOMMODATION_CODE`,
  `UNKNOWN_CONSENT_SCOPE`, `INVALID_CALLBACK_WINDOW`, `MISSING_PERSON_IDENTITY`,
  ...). The event status is `accepted_with_errors` when any item was rejected.
  There is no partial commit of the event itself: the entire record with its
  per-item verdict is committed together.
- **Consent/callback projection**: `CONTACT_CALLBACK` gates phone, email and the
  callback window; `ACCOMMODATION_TRANSFER` gates the accommodation list;
  `CASE_SUMMARY_TRANSFER` gates the free-text summary. Revocations remove
  scopes immediately from the projection.

## Out-of-order, duplicate and concurrent events

- **Out-of-order revocation**: events are replayed in a deterministic
  topological order. A revocation is projected immediately after the event it
  references, even if it was persisted first. This keeps the final state stable
  and replayable.
- **Duplicates**: identical `(eventId, payloadHash)` returns the original
  result; no new event row, no new adapter call.
- **Concurrency**: writes are serialized through a single SQLite connection
  with WAL + `busy_timeout`, and correlation is keyed by a stable
  `person_key` (hash of normalized name + DOB). Two channels reporting the same
  applicant concurrently therefore resolve to **exactly one canonical request
  and one event chain**. Explicit `canonicalRequestId` is honored when present.
- **Failed retries never re-expose withdrawn data**: retries recompute the
  projection from the durable chain, so a `CONTACT_CALLBACK` revocation already
  on record means the adapter receives a view with phone/email/callback
  redacted.

## Downstream adapter failures and retries

The downstream legal-services system is behind the `store.Adapter` interface.
The in-process `internal/adapter.Recorder` simulates it and supports
`FailOnce()`/`SetFailAll(true)` for the mixed-batch scenario.

- If the adapter returns an error on first delivery, the event is still
  durably committed with `deliveryStatus = "pending"`, event status
  `adapter_transient_failure`, attempt #1 recorded, and HTTP `202`.
- `POST /api/v1/events/{eventId}/retry` re-projects and re-attempts delivery,
  appending a new attempt record. A successful retry marks delivery
  `delivered`; another transient failure leaves it `pending` for further
  retries. Every attempt (success and failure) is stored, so the chain is
  replayable and auditable.

## Stable replay / auditability

The canonical projection is a **pure function** of the ordered event chain.
`GET /api/v1/requests/{id}` and `/audit` always recompute from durable rows and
return the same `projectionHash` for the same chain. Restarting the service,
re-pointing it at the same SQLite file, or rebuilding from an exported chain
produces an identical result. Audit summaries include event count, last
sequence, full event-id chain and projection hash.

## Data-retention decisions

- **Raw envelopes** are retained verbatim in `events.raw_envelope` for
  reproducibility of source normalization and dispute resolution.
- **Attempt logs** are retained indefinitely for the event's lifetime to support
  retry auditing.
- **Canonical requests and events are not auto-deleted.** Retention/expiry
  (e.g. after case closure or a statutory window) is intentionally left to an
  out-of-band purge job so this service never silently loses intake records.
- Contact channels are **redacted in projections** when `CONTACT_CALLBACK` is
  absent or revoked, but the original value remains in the immutable event row
  for the authorized source channel.
- SQLite files (`*.db`, `*.sqlite*`) are git-ignored.

## Testing

```bash
go test ./...          # full suite (includes -race in CI usage)
go test -race ./...
```

The suite in `internal/api/api_test.go` exercises three-channel merging,
idempotent duplicates, payload conflicts, per-item errors, contact revocation
and replay redaction, transient adapter failure + retry, retry after
revocation, concurrent single-chain formation, stable projection hashes,
missing fields and out-of-order revocation.
