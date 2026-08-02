# Accessible Intake Gateway API

Unified intake for physical-site, hotline and web legal-service channels.
Channel envelopes are normalized into one **canonical request** per applicant,
with idempotency, source correlation, consent handling and per-record errors.

All behavior is pinned by `materials/channel-contracts.json`. Channel IDs
(`PHYSICAL`, `HOTLINE`, `WEB`), consent scopes, accommodation codes and the
minute-based `callbackWindowMinutes` field are used verbatim — never renamed.

Base URL: `http://localhost:8080` (default). All bodies are JSON.

## Model

### Envelope (what channels POST)

| Field | Type | INTAKE | REVOCATION | Notes |
|---|---|---|---|---|
| `eventId` | string | required | required | Idempotency key. |
| `eventType` | string | optional | optional | `INTAKE` (default) or `REVOCATION`; `revokes` present implies `REVOCATION`. |
| `channel` | string | required | optional | One of `PHYSICAL`, `HOTLINE`, `WEB`. |
| `deskReceiptNo` / `callRef` / `submissionId` | string | required | – | Source correlation field, per channel contract (`sourceIdField`). |
| `visitor` / `caller` / `applicant` | object | optional | – | Person object, per channel contract (`personField`). |
| `accommodations` | string[] | optional | – | Must be registered codes. |
| `consent` | string[] | optional | – | Must be registered scopes. |
| `callbackWindowMinutes` | integer ≥ 0 | HOTLINE only | – | Minute-based callback window. |
| `revokes` | string | – | required | `eventId` of the accepted event whose grant is revoked. |
| `scopes` | string[] | – | required | Non-empty subset of registered scopes. |

### Person object

```json
{"name": "Li Ming", "phone": "+86 138-0000-0000", "email": "li@example.org", "idNumber": "..."}
```

Optional, but when present it must carry a non-empty `name` and at least one
of `phone`, `email`, `idNumber` (`INVALID_PERSON` otherwise).

**Merge rule.** Events merge into one canonical request via a deterministic
person key: `SHA-256(normalized name + strongest identifier)` where the
strongest identifier is `idNumber` > `phone` > `email`. Normalization:
name is case-folded with whitespace collapsed; phone is digits-only; email is
lower-cased. Adapters should therefore submit full international phone
formats. An event **without** a usable person identity can never be merged
safely — it anchors a standalone request keyed by its own `eventId`
(documented behavior, never a silent guess).

### Per-record result

```json
{
  "eventId": "EV-H-001",
  "status": "ACCEPTED",
  "requestId": "req_4f2a…",
  "seq": 3,
  "idempotentReplay": false,
  "retryable": false,
  "appliedScopes": ["CASE_SUMMARY_TRANSFER"],
  "effectiveConsent": ["ACCOMMODATION_TRANSFER", "CONTACT_CALLBACK"],
  "errors": [{"code": "UNKNOWN_ACCOMMODATION", "field": "accommodations", "message": "…"}]
}
```

`status` ∈ `ACCEPTED` | `REPLAYED`(attempts log) | `CONFLICT` | `FAILED`.
On replay the body is the **original stored result** plus
`idempotentReplay: true`. `errors` lists *every* field-level problem on the
record.

## Endpoints

### `POST /v1/intake/events` — single envelope

```bash
curl -X POST localhost:8080/v1/intake/events -d '{
  "eventId": "EV-H-001", "channel": "HOTLINE", "callRef": "C-19",
  "callbackWindowMinutes": 45,
  "caller": {"name": "Li Ming", "phone": "+86 138 0000 0000"},
  "consent": ["CONTACT_CALLBACK"]
}'
```

| HTTP | Meaning |
|---|---|
| 201 | Accepted; event appended to the chain, state folded. |
| 200 | Idempotent replay (same `eventId`, same payload) — original result. |
| 409 | Conflict: same `eventId`, different payload (`PAYLOAD_CONFLICT`). |
| 422 | Validation failure; `errors` lists every problem. |
| 503 | Transient failure; `retryable: true`, safe to retry as-is. |
| 400 | Body is not JSON. |

Revocation example:

```bash
curl -X POST localhost:8080/v1/intake/events -d '{
  "eventId": "EV-W-002", "revokes": "EV-W-001", "scopes": ["CASE_SUMMARY_TRANSFER"]
}'
```

### `POST /v1/intake/batches` — mixed batch

Body: `{"records": [ <envelope>, … ]}` (a bare JSON array also works), at most
500 records, 1 MiB total.

**Always returns 200** with `{"results": […]}` in submission order. Each
record runs in its **own transaction**, so a batch may freely mix successes,
validation failures, conflicts, replays and retryable failures; no record
affects any other. Records are processed in array order — an out-of-order
revocation fails retryable (`REVOKES_UNKNOWN_EVENT`) and can be resent after
its grant lands.

### `GET /v1/requests/{requestId}` — canonical projection

```json
{
  "requestId": "req_4f2a…",
  "canonicalVersion": "intake.v1",
  "person": {"name": "Li Ming"},
  "contact": {"phone": "+86 138 0000 0000"},
  "channels": {
    "PHYSICAL": {"correlationField": "deskReceiptNo", "correlationId": "D-88", "eventIds": ["EV-P-001"]},
    "HOTLINE":  {"correlationField": "callRef", "correlationId": "C-19", "eventIds": ["EV-H-001"], "callbackWindowMinutes": 45},
    "WEB":      {"correlationField": "submissionId", "correlationId": "W-71", "eventIds": ["EV-W-001"]}
  },
  "accommodations": ["BRAILLE_MATERIAL"],
  "consentEffective": ["ACCOMMODATION_TRANSFER", "CONTACT_CALLBACK"],
  "consentRevoked": ["CASE_SUMMARY_TRANSFER"],
  "callbackWindowMinutes": 45,
  "eventCount": 4,
  "createdAt": "…", "updatedAt": "…"
}
```

`contact` (phone/email) is present **only while `CONTACT_CALLBACK` is
effective**. Revoking it hides contact details immediately, and replaying an
older grant never brings them back. `idNumber` is never exposed. 404 when the
request does not exist.

### `GET /v1/requests/{requestId}/events` — event chain

`{"events": […]}` ordered by `seq`: `seq`, `eventId`, `eventType`, `channel`,
`correlationId`, `payloadHash`, `appliedAt`, and the canonicalized original
`payload`. Byte-stable across reads.

### `GET /v1/requests/{requestId}/audit` — minimal audit summary

```json
{
  "requestId": "req_4f2a…", "canonicalVersion": "intake.v1",
  "personKey": "person:7c9e…", "eventCount": 4,
  "firstSeq": 1, "lastSeq": 4,
  "channels": ["HOTLINE", "PHYSICAL", "WEB"],
  "correlations": {"PHYSICAL": "D-88", "HOTLINE": "C-19", "WEB": "W-71"},
  "accommodations": ["BRAILLE_MATERIAL"],
  "consentEffective": ["ACCOMMODATION_TRANSFER", "CONTACT_CALLBACK"],
  "consentRevoked": ["CASE_SUMMARY_TRANSFER"],
  "stateHash": "sha256-of-folded-state",
  "createdAt": "…", "updatedAt": "…"
}
```

`stateHash` pins the exact folded state so replays can be compared byte for
byte.

### `GET /v1/requests/{requestId}/attempts` — every attempt

`{"attempts": […]}` ordered by `id`: every submission touching the request's
event keys, accepted or not — `outcome` ∈ `ACCEPTED` | `REPLAYED` |
`CONFLICT` | `VALIDATION_FAILED` | `TRANSIENT_FAILURE`, plus `payloadHash`
and error detail. Attempts that failed before their event was accepted appear
once the event exists.

### `GET /healthz`

`{"status": "ok", "canonicalVersion": "intake.v1"}`

## Transaction semantics (per record)

1. **One record, one transaction.** Event insert, state fold and attempt log
   commit or roll back together.
2. **Exactly-once application.** An accepted event mutates state once. Same
   `eventId` + same payload (canonical JSON hash) returns the stored original
   result; same `eventId` + different payload returns 409 and changes nothing
   but the attempts log.
3. **Failures never consume the key.** Validation failures persist only an
   attempt row; a corrected retry with the same `eventId` is processed on its
   own merits.
4. **Transient failures are retryable.** Adapter/DB hiccups roll back fully,
   return `retryable: true` (503 single / `FAILED` item in batch) and are
   logged as `TRANSIENT_FAILURE`.
5. **Concurrency.** `canonical_requests.person_key` is `UNIQUE`; two channels
   reporting the same applicant concurrently resolve to one request and one
   `seq`-ordered event chain.

## Error codes

`INVALID_JSON`, `INVALID_ENVELOPE`, `FIELD_TYPE_MISMATCH`, `MISSING_EVENT_ID`,
`INVALID_EVENT_TYPE`, `MISSING_CHANNEL`, `UNKNOWN_CHANNEL`,
`MISSING_CORRELATION_ID`, `INVALID_PERSON`, `UNKNOWN_ACCOMMODATION`,
`UNKNOWN_CONSENT_SCOPE`, `INVALID_CALLBACK_WINDOW`,
`INVALID_FIELD_FOR_CHANNEL`, `MISSING_REVOKES`, `MISSING_SCOPES`,
`REVOKES_UNKNOWN_EVENT` (retryable — grant not landed yet),
`PAYLOAD_CONFLICT` (409), `TRANSIENT_FAILURE` (retryable, 503),
`NOT_FOUND` (404).
