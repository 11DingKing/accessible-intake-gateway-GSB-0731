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

### `GET /v1/requests/{key}/match-evidence` — identity resolution evidence

Returns the deterministic `MatchDecision` for every event bound to the request,
ordered by chain position (see *Identity resolution* below).

### `GET /v1/events/{id}/attempts` — attempt history for one event

Every processing attempt (including retries) in order.

### `GET /healthz`

Liveness plus the loaded `canonicalVersion`.

---

## Identity resolution (hotline callbacks)

A hotline callback may arrive **before** the web submission that first carries a
correlation number, and may bring identity fragments that partly match and
partly conflict with an existing request. Resolution is deterministic and always
converges to **one** canonical chain.

### Fragments

Supply corroborating identity signals under an `identity` object; the source
correlation number is `canonicalKey` (or `correlation`):

```json
{
  "eventId": "EV-H-CALL", "channel": "HOTLINE", "callRef": "C-900",
  "callbackWindowMinutes": 30,
  "identity": {"phone": "+1 (415) 555-0100", "email": "Jamie@Example.com"}
}
```

| Fragment      | Weight | Unique | Sensitive | Notes                                   |
|---------------|--------|--------|-----------|-----------------------------------------|
| `correlation` | 1.0    | yes    | no        | the source correlation number (decisive)|
| `phone`       | 0.5    | no     | **yes**   | matched by hash; raw value consent-gated |
| `email`       | 0.5    | no     | **yes**   | matched by hash; raw value consent-gated |
| `dob`         | 0.4    | no     | no        |                                         |
| `familyName`  | 0.25   | no     | no        |                                         |
| `givenName`   | 0.2    | no     | no        |                                         |
| `postalCode`  | 0.15   | no     | no        |                                         |

### Match reasons & confidence

Each accepted event carries a `match` decision in its result and is persisted as
evidence. Confidence is `HIGH` when the correlation number matches or the summed
fragment weight ≥ 0.7, `MEDIUM` ≥ 0.4, else `LOW`.

| Reason                        | Meaning                                                                 |
|-------------------------------|-------------------------------------------------------------------------|
| `CORRELATION_MATCH`           | Correlation number resolved to an existing request (decisive).          |
| `CORROBORATED_MATCH`          | A new correlation / fragment-only event bound to an existing request via shared fragments. |
| `NEW_CORRELATION`             | Brand-new correlation number, no corroboration → new request.           |
| `NEW_FRAGMENTS`               | Fragment-only event (e.g. early callback), nothing matched → new request keyed by a synthetic `frag:` key. |
| `UNIQUE_CONFLICT_NEW_REQUEST` | Shared corroborating fragment but a **different** unique correlation → kept separate; the shared fragment is recorded in `conflictingOn`. |
| `AMBIGUOUS_MATCH`             | Fragments point at multiple requests → bound to the earliest, others recorded as conflict evidence. |
| `CHANNEL_SOURCE_FALLBACK`     | No identity signals → Round 1 `channel:sourceId` keying.                |
| `REVOCATION_TARGET`           | A revocation resolved to the request it withdraws consent from.         |

### One chain under out-of-order / concurrency / retry

- A **fragment ownership index** (`identity_fragment`, `frag_hash` primary key)
  means the first writer to claim a correlation number or fragment owns it;
  every later event carrying that signal resolves to the same request.
- An **alias index** (`request_alias`) lets a correlation number attach to a
  request that was created earlier from fragments alone, so the early callback
  and the later web build share **one** chain. Reads accept the canonical key or
  any alias.
- All writes serialize through a single SQLite connection, so concurrent claims
  from two channels and adapter-failure retries can never fork a second chain.
- Two different unique correlation numbers are **never merged**, even when they
  share a weak fragment — that is surfaced as `conflictingOn` evidence.

### Callback-window dispositions

The minute unit from Round 1 is unchanged. Negative values are `REJECTED`;
otherwise the window is classified (the disposition is non-identifying and shown
even before consent):

| Minutes    | `callbackDisposition` |
|------------|-----------------------|
| `0`        | `IMMEDIATE`           |
| `1…1439`   | `INTRADAY`            |
| `≥ 1440`   | `CROSS_DAY`           |

### Contact info is consent-gated

Raw contact methods (`phone`/`email`) and the raw `callbackWindowMinutes` are
returned **only** while `CONTACT_CALLBACK` is effective. Before consent — or
after a sticky revocation — the projection shows only the non-identifying
disposition; the raw values are never emitted, including on retries or replays.

### Revocation erases the governed data (scope-precise)

Revoking a scope does more than gate the projection — it **erases** the raw /
reversible data that scope governs, while preserving legitimate, non-reversible
event evidence. Revoking `CONTACT_CALLBACK`:

- **Deletes** the raw `contact_method` rows (phone/email) and **nulls** the exact
  `callback_window_minutes`.
- **Keeps** the coarse `callbackDisposition` bucket, the identity-fragment
  **hashes** (non-reversible), the `match_evidence`, and the full event chain.
- **Never touches** other scopes' data — `CASE_SUMMARY_TRANSFER` and its state
  are untouched, so revocation is not over-broad.

Guarantees under adversarial ordering:

- **Idempotent.** Repeating a revocation (a second revoke event, or an exact
  replay) erases nothing further and adds no duplicate consent state.
- **No resurrection.** A late-arriving or retried event that carries a value
  governed by an already-revoked scope is **suppressed** — the raw value is not
  re-stored — even though the event itself is still accepted and appended to the
  chain as legitimate evidence (audited as `CONTACT_SUPPRESSED`).
- **One chain.** A revocation resolves to the converged request via the alias
  index or the referenced event's owner, so it never forks a second chain; this
  holds under out-of-order arrival, transient-failure retries, and concurrent
  transactions.
- **Auditable, non-reversible.** Each revocation writes `CONSENT_REVOKED` and a
  `CONTACT_ERASED scopes=… clearedItems=N` summary (counts only, no raw values).

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
  `contact_detail`, `contact_method`, `request_alias`, `identity_fragment`,
  `match_evidence`, `idempotency`, `attempt`, `audit_entry`.
- **Retention decisions:**
  - `attempt` and `audit_entry` are **append-only** — the full history of every
    attempt and canonical change is retained for audit and stable replay.
  - `contact_detail` (callback window/disposition) and `contact_method` (raw
    phone/email) hold contact data but are **projected through the consent
    gate**: raw values are withheld before `CONTACT_CALLBACK` and after a sticky
    revocation, even though the rows are kept for audit continuity.
  - `identity_fragment` keeps only **hashes** of identity signals; raw sensitive
    values never enter the fragment index, match evidence, or the audit trail.
  - `request_alias` and `match_evidence` retain how each event resolved, so the
    match reasoning is replayable and auditable.
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
internal/identity/     deterministic match policy (fragments, scoring, windows)
internal/normalize/    channel envelopes -> canonical events (pure, deterministic)
internal/store/        SQLite persistence, identity resolution, convergence, projections
internal/httpapi/      net/http mux, request/response encoding
materials/             channel-contracts.json fixture (source of truth)
```
