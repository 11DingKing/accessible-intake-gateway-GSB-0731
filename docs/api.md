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

| Field                                        | Type        | INTAKE       | REVOCATION | Notes                                                                       |
| -------------------------------------------- | ----------- | ------------ | ---------- | --------------------------------------------------------------------------- |
| `eventId`                                    | string      | required     | required   | Idempotency key.                                                            |
| `eventType`                                  | string      | optional     | optional   | `INTAKE` (default) or `REVOCATION`; `revokes` present implies `REVOCATION`. |
| `channel`                                    | string      | required     | optional   | One of `PHYSICAL`, `HOTLINE`, `WEB`.                                        |
| `deskReceiptNo` / `callRef` / `submissionId` | string      | required     | –          | Source correlation field, per channel contract (`sourceIdField`).           |
| `visitor` / `caller` / `applicant`           | object      | optional     | –          | Person object, per channel contract (`personField`).                        |
| `accommodations`                             | string[]    | optional     | –          | Must be registered codes.                                                   |
| `consent`                                    | string[]    | optional     | –          | Must be registered scopes.                                                  |
| `callbackWindowMinutes`                      | integer ≥ 0 | HOTLINE only | –          | Minute-based callback window; see classification below.                     |
| `revokes`                                    | string      | –            | required   | `eventId` of the accepted event whose grant is revoked.                     |
| `scopes`                                     | string[]    | –            | required   | Non-empty subset of registered scopes.                                      |

### Callback window classification

The minute unit from round 1 is unchanged. The value is classified
deterministically from the minutes alone (the contract carries no start
time), and the class is exposed as `callbackWindowKind` on the request
projection, the per-channel link, and the audit summary:

| Value                    | Class       | Handling                                          |
| ------------------------ | ----------- | ------------------------------------------------- |
| absent                   | –           | no callback window on the request                 |
| `0`                      | `IMMEDIATE` | accepted; immediate callback                      |
| `1..1440`                | `SAME_DAY`  | accepted; window fits in one day (1440 = 24h)     |
| `>1440`                  | `CROSS_DAY` | accepted; window necessarily spans a day boundary |
| negative / fractional    | –           | rejected: `INVALID_CALLBACK_WINDOW`               |
| on a non-HOTLINE channel | –           | rejected: `INVALID_FIELD_FOR_CHANNEL`             |

### Person object

```json
{
  "name": "Li Ming",
  "phone": "+86 138-0000-0000",
  "email": "li@example.org",
  "idNumber": "..."
}
```

Optional, but when present it must carry a non-empty `name` and at least one
of `phone`, `email`, `idNumber` (`INVALID_PERSON` otherwise).

**Merge rule.** Events merge into one canonical request in two deterministic
stages, evaluated inside the record's write transaction (writes are
serialized, so concurrent channel claims can never fork a second chain):

1. **Exact key** — `SHA-256(normalized name + strongest identifier)` where
   the strongest identifier is `idNumber` > `phone` > `email`. Normalization:
   name is case-folded with whitespace collapsed; phone is digits-only; email
   is lower-cased. Adapters should submit full international phone formats.
2. **Candidate match** — when the exact key misses, normalized identity
   fragments (`name`, `phone`, `email`, `idNumber`) are compared with every
   request sharing at least one fragment. Scoring: `idNumber` match +100,
   `phone`/`email` match +50, `name` match +10; `phone`/`email` conflict
   −100, `idNumber` conflict −1000 (hard block). Rules per candidate:
   - any strong-identifier (`idNumber`/`phone`/`email`) **conflict** →
     `CONFLICT_REJECTED`, merging is blocked even if another identifier
     matches;
   - else any strong-identifier **match** → `MERGE`; the oldest candidate
     wins when several merge;
   - name-only overlap → `NAME_ONLY`, recorded but never merged.

   An event **without** a usable person identity never merges: it anchors a
   standalone request keyed by its own `eventId` (never a silent guess).

**Match evidence.** Every accepted INTAKE result carries a `match` object —
persisted with the event, so replays return it verbatim and audits replay
deterministically:

```json
"match": {
  "decision": "MERGED",
  "requestId": "req_4f2a…",
  "score": 50,
  "rationale": ["merged into req_4f2a…: phone matched with no strong-identifier conflict"],
  "considered": [
    {"requestId": "req_4f2a…", "outcome": "MERGE", "score": 50,
     "matched": [{"kind": "phone", "fragmentHash": "9f2e…"}],
     "conflicted": [{"kind": "name", "fragmentHash": "71ab…"}]}
  ]
}
```

`decision` ∈ `EXACT` | `MERGED` | `CANDIDATE` | `NEW` | `STANDALONE`.
`CANDIDATE` means partial overlap existed but was insufficient or blocked —
the event anchors a new standalone request, and `considered` records why.
Evidence exposes fragment **kinds and hashes only, never raw values**, so no
contact detail is revealed before consent is confirmed. Identity fragments
brought by merged events accumulate on the request, letting later events
match on them. After a contact erasure (below), matching also consults the
non-reversible erased-fragment evidence; such matches carry
`"redacted": true` and rationale like `phone(redacted) matched`, so a late
event carrying the revoked phone still converges to the same chain without
the database retaining the raw number.

### Contact erasure on revocation

Consent state and stored contact data are separate concerns. Effective
consent always folds from the event chain (round 1). In addition, when a
revocation removes the **last live `CONTACT_CALLBACK` grant** of a request,
contact erasure fires inside the same transaction:

- `person_json` loses `phone`/`email` (name and `idNumber` are not contact
  channels and are kept);
- live `phone`/`email` identity fragments move to
  `erased_identity_evidence` — `{kind, fragmentHash, erasedByEvent,
erasedAt}` — the legal, non-reversible proof of what was erased;
- every stored payload copy on the chain gets its person `phone`/`email`
  replaced by a `REDACTED#<fragmentHash>` marker;
- `payload_hash` columns are **never** touched: replays still verify against
  the original bytes.

Only the revoked scope's data is erased — a `CASE_SUMMARY_TRANSFER` or
`ACCOMMODATION_TRANSFER` revocation touches no contact data, and erasure
never crosses into another chain. Repeated erasure is idempotent (evidence
is `INSERT OR IGNORE`; a second revocation of an already-erased scope is an
accepted no-op). After erasure:

- later events **without** a fresh `CONTACT_CALLBACK` grant do not
  re-register contact fields or fragments, and their stored payload copy is
  redacted on arrival — a late retry carrying the revoked phone cannot
  resurrect it;
- a later event **with** a fresh `CONTACT_CALLBACK` grant is a new consent:
  contact may be registered again from that event onward;
- replays of pre-erasure events return their original stored results
  (historical evidence) but never re-fold state.

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
  "match": {
    "decision": "MERGED",
    "requestId": "req_4f2a…",
    "score": 50,
    "rationale": ["…"]
  },
  "errors": [
    {
      "code": "UNKNOWN_ACCOMMODATION",
      "field": "accommodations",
      "message": "…"
    }
  ]
}
```

`status` ∈ `ACCEPTED` | `REPLAYED`(attempts log) | `CONFLICT` | `FAILED`.
On replay the body is the **original stored result** (including its `match`
evidence) plus `idempotentReplay: true`. `errors` lists _every_ field-level
problem on the record.

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

| HTTP | Meaning                                                             |
| ---- | ------------------------------------------------------------------- |
| 201  | Accepted; event appended to the chain, state folded.                |
| 200  | Idempotent replay (same `eventId`, same payload) — original result. |
| 409  | Conflict: same `eventId`, different payload (`PAYLOAD_CONFLICT`).   |
| 422  | Validation failure; `errors` lists every problem.                   |
| 503  | Transient failure; `retryable: true`, safe to retry as-is.          |
| 400  | Body is not JSON.                                                   |

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
  "person": { "name": "Li Ming" },
  "contact": { "phone": "+86 138 0000 0000" },
  "channels": {
    "PHYSICAL": {
      "correlationField": "deskReceiptNo",
      "correlationId": "D-88",
      "eventIds": ["EV-P-001"]
    },
    "HOTLINE": {
      "correlationField": "callRef",
      "correlationId": "C-19",
      "eventIds": ["EV-H-001"],
      "callbackWindowMinutes": 45,
      "callbackWindowKind": "SAME_DAY"
    },
    "WEB": {
      "correlationField": "submissionId",
      "correlationId": "W-71",
      "eventIds": ["EV-W-001"]
    }
  },
  "accommodations": ["BRAILLE_MATERIAL"],
  "consentEffective": ["ACCOMMODATION_TRANSFER", "CONTACT_CALLBACK"],
  "consentRevoked": ["CASE_SUMMARY_TRANSFER"],
  "callbackWindowMinutes": 45,
  "callbackWindowKind": "SAME_DAY",
  "eventCount": 4,
  "createdAt": "…",
  "updatedAt": "…"
}
```

`contact` (phone/email) is present **only while `CONTACT_CALLBACK` is
effective** — before consent is confirmed, and after it is revoked, raw
contact details never leave the service (match evidence carries hashes
only). Replaying an older grant never brings contact back. `idNumber` is
never exposed. 404 when the request does not exist.

### `GET /v1/requests/{requestId}/events` — event chain

`{"events": […]}` ordered by `seq`: `seq`, `eventId`, `eventType`, `channel`,
`correlationId`, `payloadHash`, `appliedAt`, the canonicalized original
`payload`, and the persisted `match` evidence for INTAKE events. Byte-stable
across reads.

### `GET /v1/requests/{requestId}/audit` — minimal audit summary

```json
{
  "requestId": "req_4f2a…",
  "canonicalVersion": "intake.v1",
  "personKey": "person:7c9e…",
  "eventCount": 4,
  "firstSeq": 1,
  "lastSeq": 4,
  "channels": ["HOTLINE", "PHYSICAL", "WEB"],
  "correlations": { "PHYSICAL": "D-88", "HOTLINE": "C-19", "WEB": "W-71" },
  "accommodations": ["BRAILLE_MATERIAL"],
  "consentEffective": ["ACCOMMODATION_TRANSFER", "CONTACT_CALLBACK"],
  "consentRevoked": ["CASE_SUMMARY_TRANSFER"],
  "callbackWindowMinutes": 45,
  "callbackWindowKind": "SAME_DAY",
  "erasedIdentities": [
    {
      "kind": "phone",
      "fragmentHash": "9f2e…",
      "erasedByEvent": "EV-H-100",
      "erasedAt": "…"
    }
  ],
  "stateHash": "sha256-of-folded-state",
  "createdAt": "…",
  "updatedAt": "…"
}
```

`stateHash` pins the exact folded state so replays can be compared byte for
byte. `erasedIdentities` (present only after a contact erasure) is the
non-reversible compliance evidence: which fragment kinds were purged, by
which revocation event, and when — hashes only, never raw values.

### `GET /v1/requests/{requestId}/attempts` — every attempt

`{"attempts": […]}` ordered by `id`: every submission touching the request's
event keys, accepted or not — `outcome` ∈ `ACCEPTED` | `REPLAYED` |
`CONFLICT` | `VALIDATION_FAILED` | `TRANSIENT_FAILURE`, plus `payloadHash`
and error detail. Attempts that failed before their event was accepted appear
once the event exists.

### `GET /healthz`

`{"status": "ok", "canonicalVersion": "intake.v1"}`

## Transaction semantics (per record)

1. **One record, one transaction.** Event insert, state fold, contact
   erasure (when triggered) and attempt log commit or roll back together.
2. **Exactly-once application.** An accepted event mutates state once. Same
   `eventId` + same payload (canonical JSON hash) returns the stored original
   result; same `eventId` + different payload returns 409 and changes nothing
   but the attempts log.
3. **Failures never consume the key.** Validation failures persist only an
   attempt row; a corrected retry with the same `eventId` is processed on its
   own merits.
4. **Transient failures are retryable.** Adapter/DB hiccups roll back fully,
   return `retryable: true` (503 single / `FAILED` item in batch) and are
   logged as `TRANSIENT_FAILURE`; the recovery retry re-runs matching against
   committed state and converges to the same single chain.
5. **Concurrency.** `canonical_requests.person_key` is `UNIQUE`, and
   candidate matching runs inside the record's serialized write transaction:
   two channels claiming the same applicant concurrently — by exact key or by
   fuzzy identity evidence — resolve to one request and one `seq`-ordered
   event chain. Out-of-order replays never re-fold or re-match.

## Error codes

`INVALID_JSON`, `INVALID_ENVELOPE`, `FIELD_TYPE_MISMATCH`, `MISSING_EVENT_ID`,
`INVALID_EVENT_TYPE`, `MISSING_CHANNEL`, `UNKNOWN_CHANNEL`,
`MISSING_CORRELATION_ID`, `INVALID_PERSON`, `UNKNOWN_ACCOMMODATION`,
`UNKNOWN_CONSENT_SCOPE`, `INVALID_CALLBACK_WINDOW`,
`INVALID_FIELD_FOR_CHANNEL`, `MISSING_REVOKES`, `MISSING_SCOPES`,
`REVOKES_UNKNOWN_EVENT` (retryable — grant not landed yet),
`PAYLOAD_CONFLICT` (409), `TRANSIENT_FAILURE` (retryable, 503),
`NOT_FOUND` (404).
