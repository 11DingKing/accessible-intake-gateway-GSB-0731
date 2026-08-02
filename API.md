# Accessible Intake Gateway — API & Operations

A unified acceptance API that normalizes physical-site, hotline, and web
legal-service intake events into a single **canonical request** model with
idempotency, cross-channel correlation, consent handling (including sticky
revocation), and per-record validation results.

- **Runtime:** Go 1.24 (auto-selected via `go.mod`), standard-library HTTP only.
- **Storage:** SQLite via the pure-Go `modernc.org/sqlite` driver (no cgo).
- **No** web framework and **no** message broker.

The channel contract in [`materials/channel-contracts.json`](materials/channel-contracts.json)
is the single source of truth for channel IDs, consent scopes, accommodation
codes, and the minute-based callback field. None of these are renamed.

---

## Running

```bash
go test ./...              # full unit + end-to-end suite
go run ./cmd/server        # serves on :8080, DB file ./intake.db
```

Environment overrides:

| Variable          | Default                            | Purpose                          |
|-------------------|------------------------------------|----------------------------------|
| `INTAKE_ADDR`     | `:8080`                            | HTTP listen address              |
| `INTAKE_DB`       | `intake.db`                        | SQLite file path (`:memory:` ok) |
| `INTAKE_CONTRACT` | `materials/channel-contracts.json` | Channel contract fixture         |

---

## Canonical model

Every accepted event is folded into a `CanonicalRequest`, correlated by a
`canonicalKey`. Supply `canonicalKey` on each envelope as the cross-channel
correlation number (e.g. the applicant/case reference). When it is omitted, an
event forms its own single-event request keyed by `channel:sourceId`, and a
revocation falls back to the `revokes` event id.

```jsonc
{
  "canonicalKey": "APP-1",
  "canonicalVersion": "intake.v1",
  "chain": [ /* one ordered link per accepted distinct event */ ],
  "accommodations": ["BRAILLE_MATERIAL"],
  "effectiveConsent": ["CASE_SUMMARY_TRANSFER"],
  "revokedConsent": ["CONTACT_CALLBACK"],
  "contact": { "exposed": false }        // callback window only shown while CONTACT_CALLBACK is effective
}
```

### Channel envelopes

Each channel keeps its own source-id and person field names (never renamed):

| Channel    | `sourceIdField` | `personField` | Extra                       |
|------------|-----------------|---------------|-----------------------------|
| `PHYSICAL` | `deskReceiptNo` | `visitor`     | —                           |
| `HOTLINE`  | `callRef`       | `caller`      | `callbackWindowMinutes`     |
| `WEB`      | `submissionId`  | `applicant`   | —                           |

A **revocation** envelope carries `revokes` (a prior eventId) and `scopes`
(consent scopes to withdraw) instead of channel source fields.

---

## Endpoints

### `POST /v1/intake/events` — submit a batch

Request:

```json
{
  "events": [
    {"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER"],"canonicalKey":"APP-1"},
    {"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"],"canonicalKey":"APP-1"},
    {"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","accommodations":["BRAILLE_MATERIAL"],"canonicalKey":"APP-1"}
  ]
}
```

Each event is processed **independently in its own transaction**, so one bad or
failing record never rolls back its neighbours. Ordering within the batch does
not matter — convergence is by `canonicalKey`, not arrival order. Response:

```jsonc
{
  "results": [
    {"eventId":"EV-W-001","channel":"WEB","status":"ACCEPTED","canonicalKey":"APP-1","attemptNo":1},
    {"eventId":"EV-W-001","channel":"WEB","status":"ACCEPTED","canonicalKey":"APP-1","duplicate":true,"attemptNo":2},
    {"eventId":"EV-W-001","channel":"WEB","status":"CONFLICT","attemptNo":3,"errors":[{"field":"eventId","code":"CONFLICT","message":"..."}]},
    {"eventId":"EV-H-002","channel":"HOTLINE","status":"REJECTED","attemptNo":1,"errors":[{"field":"callbackWindowMinutes","code":"INVALID_VALUE","message":"..."}]}
  ],
  "summary": {"total":4,"accepted":1,"duplicate":1,"conflict":1,"rejected":1,"failed":0}
}
```

### Per-record status semantics

| Status      | Meaning                                                              | Idempotent? | Retriable? |
|-------------|---------------------------------------------------------------------|-------------|------------|
| `ACCEPTED`  | Validated and applied to a canonical request.                       | yes         | —          |
| `REJECTED`  | Validation failed (missing field, unknown code, negative window…).  | yes         | no         |
| `CONFLICT`  | Same `eventId` seen before with a **different** payload.            | yes         | no (fix payload) |
| `FAILED`    | Transient downstream adapter error; nothing persisted.              | no          | **yes**    |

- **Same event key + same payload → replay.** The stored result is returned
  verbatim with `duplicate: true`; no new chain link is created.
- **Same event key + different payload → `CONFLICT`.** The original is never
  overwritten.

### `GET /v1/requests/{key}` — canonical request

Returns the fully projected `CanonicalRequest` (chain, accommodations,
effective/revoked consent, contact projection).

### `GET /v1/requests/{key}/summary` — minimal audit summary

Returns the projected request plus the append-only `attempts` log, the
`auditTrail`, `acceptedCount` (distinct accepted events), and a **replay-stable
`digest`**. The digest is a SHA-256 over the normalized result and the ordered
attempt/audit history with volatile timestamps and row ids excluded, so
replaying the same batch into a fresh database yields the same digest.

### `GET /v1/events/{id}/attempts` — attempt history for one event

Every processing attempt (including retries) in order.

### `GET /healthz`

Liveness plus the loaded `canonicalVersion`.

---

## Idempotency, correlation & the event chain

- **Idempotency key** = `(eventId, payloadHash)`. `payloadHash` is a SHA-256 of
  the envelope re-encoded in canonical (key-sorted) JSON, so re-ordered but
  otherwise identical payloads hash identically.
- **Source correlation** is preserved per link (`sourceIdField` + `sourceId`,
  e.g. `deskReceiptNo=D-88`).
- **One chain per canonical request.** The `event_link` table has a
  `UNIQUE(request_id, event_id)` constraint and all writes serialize through a
  single SQLite connection, so two channels reporting the same canonical request
  concurrently converge to exactly one chain — the second insert of the same
  event is ignored, never duplicated.

## Consent & sticky revocation

- Grants add scopes to the consent ledger. A grant **never** re-activates a
  scope that is already `REVOKED`.
- Revocations are **sticky and terminal** for a scope. Once `CONTACT_CALLBACK`
  is revoked, the callback contact projection is withdrawn (`contact.exposed =
  false`) and **no retry or replay of the original grant can re-expose it**.

## Retry behavior

A downstream adapter (`store.Downstream`) may be wired in front of canonical
persistence. If it returns a `*store.TransientError`, the event is reported
`FAILED`/`retriable` and **no canonical change is committed** — only a `FAILED`
attempt is logged. Re-submitting the same envelope later performs a real apply
(not a duplicate replay), and the attempt log shows the failure followed by the
success. Non-transient downstream errors are terminal (`REJECTED`).

---

## Persistence & data retention

- **Setup is repeatable.** [`internal/store/schema.sql`](internal/store/schema.sql)
  is embedded and applied on every `store.Open`; every statement uses
  `IF NOT EXISTS`, so opening a fresh or existing database always converges to
  the same shape without data loss.
- **Tables:** `canonical_request`, `event_link`, `accommodation`, `consent`,
  `contact_detail`, `idempotency`, `attempt`, `audit_entry`.
- **Retention decisions:**
  - `attempt` and `audit_entry` are **append-only** — the full history of every
    attempt and canonical change is retained for audit and stable replay.
  - `contact_detail` holds only the minute-based callback window and is
    **projected out** (not returned) once `CONTACT_CALLBACK` is revoked, so
    withdrawn contact data is not re-exposed even though the raw row is kept for
    audit continuity.
  - The `idempotency` ledger retains one row per `(eventId, payloadHash)` so
    results stay stable and conflicts remain detectable indefinitely.
  - `*.db`, `*.sqlite*`, and `coverage.out` are git-ignored; the database is
    local state, not source.

---

## Layout

```
cmd/server/            HTTP entrypoint (stdlib, graceful shutdown)
internal/contract/     loads & validates channel-contracts.json
internal/model/        canonical request domain types
internal/normalize/    channel envelopes -> canonical events (pure, deterministic)
internal/store/        SQLite persistence, idempotency, convergence, projections
internal/httpapi/      net/http mux, request/response encoding
materials/             channel-contracts.json fixture (source of truth)
```
