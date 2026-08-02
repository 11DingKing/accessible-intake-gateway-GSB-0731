-- Accessible Intake Gateway schema.
-- Repeatable: every statement is idempotent (IF NOT EXISTS), so applying this
-- file to a fresh or existing database always converges to the same shape.

-- One row per converged applicant request. The canonical_key correlates
-- events across the physical, hotline, and web channels.
CREATE TABLE IF NOT EXISTS canonical_request (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_key TEXT NOT NULL UNIQUE,
    version       TEXT NOT NULL,
    created_seq   INTEGER NOT NULL,
    updated_seq   INTEGER NOT NULL
);

-- Append-only chain of accepted events for a canonical request. UNIQUE on
-- (request_id, event_id) guarantees a single event contributes to one chain
-- exactly once even under concurrent submission.
CREATE TABLE IF NOT EXISTS event_link (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id      INTEGER NOT NULL REFERENCES canonical_request(id),
    event_id        TEXT NOT NULL,
    channel         TEXT NOT NULL,
    kind            TEXT NOT NULL,
    source_id_field TEXT,
    source_id       TEXT,
    payload_hash    TEXT NOT NULL,
    UNIQUE (request_id, event_id)
);

-- Effective accommodations per request (set semantics via UNIQUE).
CREATE TABLE IF NOT EXISTS accommodation (
    request_id INTEGER NOT NULL REFERENCES canonical_request(id),
    code       TEXT NOT NULL,
    UNIQUE (request_id, code)
);

-- Consent ledger. state is GRANTED or REVOKED. revoked_at_seq being non-null
-- and >= granted_at_seq marks a scope as withdrawn. Revocation is sticky: once
-- a scope is REVOKED it is never silently re-exposed by a later retry or a
-- replayed grant of the same or earlier event.
CREATE TABLE IF NOT EXISTS consent (
    request_id     INTEGER NOT NULL REFERENCES canonical_request(id),
    scope          TEXT NOT NULL,
    state          TEXT NOT NULL,
    granted_at_seq INTEGER,
    revoked_at_seq INTEGER,
    UNIQUE (request_id, scope)
);

-- Callback contact detail, exposed only while CONTACT_CALLBACK is effective.
CREATE TABLE IF NOT EXISTS contact_detail (
    request_id              INTEGER PRIMARY KEY REFERENCES canonical_request(id),
    callback_window_minutes INTEGER,
    callback_disposition    TEXT
);

-- Raw contact methods (phone/email) held for a request. These are the original
-- contact values; they are NEVER emitted before CONTACT_CALLBACK consent is
-- effective, and never after it is revoked (sticky). Kept for audit continuity
-- but always projected through the consent gate.
CREATE TABLE IF NOT EXISTS contact_method (
    request_id INTEGER NOT NULL REFERENCES canonical_request(id),
    method     TEXT NOT NULL,
    value      TEXT NOT NULL,
    UNIQUE (request_id, method)
);

-- Alias index: every key a request is known by (its canonical key plus any
-- correlation numbers later attached to it) maps to the one request. Resolution
-- and reads go through this table, so a correlation number that arrives after a
-- fragment-only request was created still lands on the same single chain.
CREATE TABLE IF NOT EXISTS request_alias (
    alias_key  TEXT PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES canonical_request(id)
);

-- Identity fragment ownership index. frag_hash is the PRIMARY KEY, so the first
-- request to claim a fragment owns it; a later event carrying the same fragment
-- deterministically resolves to that request. This is what makes out-of-order,
-- concurrent, and retried claims converge to a single chain.
CREATE TABLE IF NOT EXISTS identity_fragment (
    frag_hash  TEXT PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES canonical_request(id),
    frag_type  TEXT NOT NULL,
    is_unique  INTEGER NOT NULL DEFAULT 0
);

-- Deterministic match evidence per event: which request it bound to, the match
-- reason, confidence label/score, and the fragment types matched or conflicted.
-- No raw contact values are stored here.
CREATE TABLE IF NOT EXISTS match_evidence (
    event_id       TEXT PRIMARY KEY,
    request_id     INTEGER NOT NULL REFERENCES canonical_request(id),
    canonical_key  TEXT NOT NULL,
    reason         TEXT NOT NULL,
    confidence     TEXT NOT NULL,
    score          REAL NOT NULL,
    matched_on     TEXT,
    conflicting_on TEXT,
    new_request    INTEGER NOT NULL DEFAULT 0,
    at             TEXT NOT NULL
);

-- Idempotency ledger: one row per (event_id, payload_hash). A repeated event
-- with the same payload replays the stored result; the same event_id with a
-- different payload_hash is a conflict.
CREATE TABLE IF NOT EXISTS idempotency (
    event_id     TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status       TEXT NOT NULL,
    canonical_key TEXT,
    result_json  TEXT NOT NULL,
    PRIMARY KEY (event_id, payload_hash)
);

-- Append-only record of every processing attempt (including retries).
CREATE TABLE IF NOT EXISTS attempt (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      TEXT NOT NULL,
    attempt_no    INTEGER NOT NULL,
    canonical_key TEXT,
    status        TEXT NOT NULL,
    payload_hash  TEXT NOT NULL,
    errors_json   TEXT,
    at            TEXT NOT NULL
);

-- Append-only audit trail scoped to a canonical request.
CREATE TABLE IF NOT EXISTS audit_entry (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_key TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    action        TEXT NOT NULL,
    detail        TEXT,
    at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_link_request ON event_link(request_id);
CREATE INDEX IF NOT EXISTS idx_attempt_event ON attempt(event_id);
CREATE INDEX IF NOT EXISTS idx_audit_key ON audit_entry(canonical_key);
CREATE INDEX IF NOT EXISTS idx_alias_request ON request_alias(request_id);
CREATE INDEX IF NOT EXISTS idx_fragment_request ON identity_fragment(request_id);
