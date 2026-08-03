package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// schemaDDL is migration v1.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS canonical_requests (
    request_id  TEXT PRIMARY KEY,
    person_key  TEXT NOT NULL UNIQUE,
    person_json TEXT NOT NULL DEFAULT '',
    state_json  TEXT NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    request_id     TEXT NOT NULL REFERENCES canonical_requests(request_id),
    event_type     TEXT NOT NULL,
    channel        TEXT NOT NULL DEFAULT '',
    correlation_id TEXT NOT NULL DEFAULT '',
    payload_json   TEXT NOT NULL,
    payload_hash   TEXT NOT NULL,
    result_json    TEXT NOT NULL DEFAULT '',
    applied_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_request ON events(request_id, seq);

CREATE TABLE IF NOT EXISTS attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    outcome      TEXT NOT NULL,
    detail_json  TEXT NOT NULL DEFAULT '',
    request_id   TEXT NOT NULL DEFAULT '',
    attempted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id, id);
`

// identityDDL is migration v2: normalized identity fragments per request,
// powering deterministic candidate matching across channels.
const identityDDL = `
CREATE TABLE IF NOT EXISTS request_identities (
    request_id    TEXT NOT NULL REFERENCES canonical_requests(request_id),
    kind          TEXT NOT NULL,
    value         TEXT NOT NULL,
    fragment_hash TEXT NOT NULL,
    PRIMARY KEY (request_id, kind, value)
);
CREATE INDEX IF NOT EXISTS idx_request_identities_lookup ON request_identities(kind, value);
`

// erasureDDL is migration v3: revocation-driven contact erasure. The flag
// marks requests whose contact data was purged; the evidence table keeps the
// non-reversible hashes of erased fragments so matching still converges to
// the same single chain without retaining raw values.
const erasureDDL = `
ALTER TABLE canonical_requests ADD COLUMN contact_erased INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS erased_identity_evidence (
    request_id      TEXT NOT NULL REFERENCES canonical_requests(request_id),
    kind            TEXT NOT NULL,
    fragment_hash   TEXT NOT NULL,
    erased_by_event TEXT NOT NULL,
    erased_at       TEXT NOT NULL,
    PRIMARY KEY (request_id, kind, fragment_hash)
);
CREATE INDEX IF NOT EXISTS idx_erased_evidence_lookup ON erased_identity_evidence(kind, fragment_hash);
`

// migration is one ordered, recorded schema step.
type migration struct {
	version int
	apply   func(ctx context.Context, db *sql.DB) error
}

func applySQL(ddl string) func(ctx context.Context, db *sql.DB) error {
	return func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, ddl)
		return err
	}
}

// migrations run in version order; each is applied at most once.
var migrations = []migration{
	{1, applySQL(schemaDDL)},
	{2, func(ctx context.Context, db *sql.DB) error {
		if _, err := db.ExecContext(ctx, identityDDL); err != nil {
			return err
		}
		return backfillIdentities(ctx, db)
	}},
	{3, applySQL(erasureDDL)},
}

// backfillIdentities registers fragments for requests stored before v2.
// INSERT OR IGNORE keeps the backfill safe to re-run.
func backfillIdentities(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT request_id, person_json FROM canonical_requests WHERE person_json != ''`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type row struct{ id, personJSON string }
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.personJSON); err != nil {
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, r := range pending {
		var p Person
		if err := json.Unmarshal([]byte(r.personJSON), &p); err != nil {
			return fmt.Errorf("backfill %s: %w", r.id, err)
		}
		for _, f := range personFragments(&p) {
			if _, err := db.ExecContext(ctx,
				`INSERT OR IGNORE INTO request_identities (request_id, kind, value, fragment_hash) VALUES (?,?,?,?)`,
				r.id, f.Kind, f.Value, fragmentHash(f.Kind, f.Value)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Store wraps the SQLite database. A single connection serializes writes;
// uniqueness constraints are the real guard against duplicate chains.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the SQLite database at dsn, e.g.
// "file:intake.db?_pragma=busy_timeout(5000)&_txlock=immediate".
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return &Store{db: db}, nil
}

// Migrate applies pending migrations in version order. It is safe to run
// any number of times: applied versions are recorded and skipped.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	for _, m := range migrations {
		var applied int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, m.version).Scan(&applied); err != nil {
			return fmt.Errorf("check schema version %d: %w", m.version, err)
		}
		if applied > 0 {
			continue
		}
		if err := m.apply(ctx, s.db); err != nil {
			return fmt.Errorf("apply migration %d: %w", m.version, err)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (?)`, m.version); err != nil {
			return fmt.Errorf("record schema version %d: %w", m.version, err)
		}
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Begin starts an immediate transaction (the DSN requests _txlock=immediate).
func (s *Store) Begin(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

// ExecOutsideTx runs a statement without an explicit transaction; used for
// best-effort attempt logging after a rolled-back transaction.
func (s *Store) ExecOutsideTx(ctx context.Context, query string, args ...any) error {
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// RequestRow is the persisted canonical request.
type RequestRow struct {
	RequestID     string
	PersonKey     string
	PersonJSON    string
	StateJSON     string
	EventCount    int
	ContactErased bool
	CreatedAt     string
	UpdatedAt     string
}

// EventRow is one accepted event on a request's chain.
type EventRow struct {
	Seq           int64
	EventID       string
	RequestID     string
	EventType     string
	Channel       string
	CorrelationID string
	PayloadJSON   string
	PayloadHash   string
	ResultJSON    string
	AppliedAt     string
}

// AttemptRow is one submission attempt, accepted or not.
type AttemptRow struct {
	ID          int64
	EventID     string
	PayloadHash string
	Outcome     string
	DetailJSON  string
	RequestID   string
	AttemptedAt string
}

const requestCols = `request_id, person_key, person_json, state_json, event_count, contact_erased, created_at, updated_at`

func scanRequest(row interface{ Scan(...any) error }) (*RequestRow, error) {
	var r RequestRow
	var erased int
	if err := row.Scan(&r.RequestID, &r.PersonKey, &r.PersonJSON, &r.StateJSON, &r.EventCount, &erased, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	r.ContactErased = erased != 0
	return &r, nil
}

// GetEventTx fetches an accepted event by idempotency key; nil when absent.
func GetEventTx(ctx context.Context, tx *sql.Tx, eventID string) (*EventRow, error) {
	row := tx.QueryRowContext(ctx, `SELECT seq, event_id, request_id, event_type, channel, correlation_id, payload_json, payload_hash, result_json, applied_at FROM events WHERE event_id = ?`, eventID)
	var e EventRow
	if err := row.Scan(&e.Seq, &e.EventID, &e.RequestID, &e.EventType, &e.Channel, &e.CorrelationID, &e.PayloadJSON, &e.PayloadHash, &e.ResultJSON, &e.AppliedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &e, nil
}

// GetRequestByIDTx fetches a request by ID; nil when absent.
func GetRequestByIDTx(ctx context.Context, tx *sql.Tx, requestID string) (*RequestRow, error) {
	r, err := scanRequest(tx.QueryRowContext(ctx, `SELECT `+requestCols+` FROM canonical_requests WHERE request_id = ?`, requestID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return r, nil
}

// InsertRequestIfAbsentTx inserts a new request, or adopts the existing one
// when another channel won the race on the same person key. Either way the
// caller gets the single authoritative request row.
func InsertRequestIfAbsentTx(ctx context.Context, tx *sql.Tx, r *RequestRow) (*RequestRow, bool, error) {
	erased := 0
	if r.ContactErased {
		erased = 1
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO canonical_requests (`+requestCols+`) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(person_key) DO NOTHING`,
		r.RequestID, r.PersonKey, r.PersonJSON, r.StateJSON, r.EventCount, erased, r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return nil, false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return r, true, nil
	}
	existing, err := scanRequest(tx.QueryRowContext(ctx, `SELECT `+requestCols+` FROM canonical_requests WHERE person_key = ?`, r.PersonKey))
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

// UpdateRequestStateTx persists the folded state after an accepted event.
func UpdateRequestStateTx(ctx context.Context, tx *sql.Tx, requestID, stateJSON string, eventCount int, updatedAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE canonical_requests SET state_json = ?, event_count = ?, updated_at = ? WHERE request_id = ?`, stateJSON, eventCount, updatedAt, requestID)
	return err
}

// InsertEventTx appends an accepted event and returns its chain sequence.
func InsertEventTx(ctx context.Context, tx *sql.Tx, e *EventRow) (int64, error) {
	res, err := tx.ExecContext(ctx, `INSERT INTO events (event_id, request_id, event_type, channel, correlation_id, payload_json, payload_hash, result_json, applied_at) VALUES (?,?,?,?,?,?,?,?,?)`,
		e.EventID, e.RequestID, e.EventType, e.Channel, e.CorrelationID, e.PayloadJSON, e.PayloadHash, e.ResultJSON, e.AppliedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// SetEventResultTx stores the original result snapshot next to the event.
func SetEventResultTx(ctx context.Context, tx *sql.Tx, seq int64, resultJSON string) error {
	_, err := tx.ExecContext(ctx, `UPDATE events SET result_json = ? WHERE seq = ?`, resultJSON, seq)
	return err
}

// InsertAttemptTx logs one submission attempt inside the current transaction.
func InsertAttemptTx(ctx context.Context, tx *sql.Tx, a *AttemptRow) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO attempts (event_id, payload_hash, outcome, detail_json, request_id, attempted_at) VALUES (?,?,?,?,?,?)`,
		a.EventID, a.PayloadHash, a.Outcome, a.DetailJSON, a.RequestID, a.AttemptedAt)
	return err
}

// GetRequest fetches a request by ID outside a transaction; nil when absent.
func (s *Store) GetRequest(ctx context.Context, requestID string) (*RequestRow, error) {
	r, err := scanRequest(s.db.QueryRowContext(ctx, `SELECT `+requestCols+` FROM canonical_requests WHERE request_id = ?`, requestID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return r, nil
}

// ListEvents returns the request's event chain in stable sequence order.
func (s *Store) ListEvents(ctx context.Context, requestID string) ([]EventRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq, event_id, request_id, event_type, channel, correlation_id, payload_json, payload_hash, result_json, applied_at FROM events WHERE request_id = ? ORDER BY seq`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EventRow{}
	for rows.Next() {
		var e EventRow
		if err := rows.Scan(&e.Seq, &e.EventID, &e.RequestID, &e.EventType, &e.Channel, &e.CorrelationID, &e.PayloadJSON, &e.PayloadHash, &e.ResultJSON, &e.AppliedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListAttemptsForRequest returns every attempt whose event ID belongs to the
// request's chain, in stable logging order. Attempts that failed before the
// event was accepted are included once the event exists.
func (s *Store) ListAttemptsForRequest(ctx context.Context, requestID string) ([]AttemptRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id, a.event_id, a.payload_hash, a.outcome, a.detail_json, a.request_id, a.attempted_at
		FROM attempts a
		WHERE a.event_id IN (SELECT event_id FROM events WHERE request_id = ?)
		ORDER BY a.id`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AttemptRow{}
	for rows.Next() {
		var a AttemptRow
		if err := rows.Scan(&a.ID, &a.EventID, &a.PayloadHash, &a.Outcome, &a.DetailJSON, &a.RequestID, &a.AttemptedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetRequestByPersonKeyTx finds a request by its exact identity key.
func GetRequestByPersonKeyTx(ctx context.Context, tx *sql.Tx, personKey string) (*RequestRow, error) {
	r, err := scanRequest(tx.QueryRowContext(ctx, `SELECT `+requestCols+` FROM canonical_requests WHERE person_key = ?`, personKey))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return r, nil
}

// FindCandidateRequestsTx returns requests sharing at least one identity
// fragment with the event — live fragments or non-reversible erased evidence
// alike — in deterministic order (oldest first).
func FindCandidateRequestsTx(ctx context.Context, tx *sql.Tx, frags []fragment) ([]*RequestRow, error) {
	if len(frags) == 0 {
		return nil, nil
	}
	cols := strings.Join([]string{
		"request_id", "person_key", "person_json", "state_json", "event_count", "contact_erased", "created_at", "updated_at",
	}, ", r.")
	livePairs := make([]string, 0, len(frags))
	evidencePairs := make([]string, 0, len(frags))
	args := make([]any, 0, len(frags)*4)
	for _, f := range frags {
		livePairs = append(livePairs, "(?,?)")
		args = append(args, f.Kind, f.Value)
	}
	for _, f := range frags {
		evidencePairs = append(evidencePairs, "(?,?)")
		args = append(args, f.Kind, fragmentHash(f.Kind, f.Value))
	}
	query := `SELECT DISTINCT r.` + cols + `
		FROM canonical_requests r
		JOIN request_identities i ON i.request_id = r.request_id
		WHERE (i.kind, i.value) IN (` + strings.Join(livePairs, ",") + `)
		UNION
		SELECT DISTINCT r.` + cols + `
		FROM canonical_requests r
		JOIN erased_identity_evidence e ON e.request_id = r.request_id
		WHERE (e.kind, e.fragment_hash) IN (` + strings.Join(evidencePairs, ",") + `)
		ORDER BY 7, 1` // created_at, request_id (positional for compound SELECT)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RequestRow
	for rows.Next() {
		r, err := scanRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListIdentitiesTx returns all identity fragments registered on a request.
func ListIdentitiesTx(ctx context.Context, tx *sql.Tx, requestID string) ([]fragment, error) {
	rows, err := tx.QueryContext(ctx, `SELECT kind, value FROM request_identities WHERE request_id = ? ORDER BY kind, value`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fragment
	for rows.Next() {
		var f fragment
		if err := rows.Scan(&f.Kind, &f.Value); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// AddIdentitiesTx registers fragments on a request; existing ones are kept.
func AddIdentitiesTx(ctx context.Context, tx *sql.Tx, requestID string, frags []fragment) error {
	for _, f := range frags {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO request_identities (request_id, kind, value, fragment_hash) VALUES (?,?,?,?)`,
			requestID, f.Kind, f.Value, fragmentHash(f.Kind, f.Value)); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRequestPersonTx persists the accumulated person record.
func UpdateRequestPersonTx(ctx context.Context, tx *sql.Tx, requestID, personJSON string) error {
	_, err := tx.ExecContext(ctx, `UPDATE canonical_requests SET person_json = ? WHERE request_id = ?`, personJSON, requestID)
	return err
}

// IdentityRow is one stored identity fragment with its hash.
type IdentityRow struct {
	Kind         string
	Value        string
	FragmentHash string
}

// ErasureRow is the non-reversible evidence of one erased fragment.
type ErasureRow struct {
	Kind          string
	FragmentHash  string
	ErasedByEvent string
	ErasedAt      string
}

// ListIdentityRowsTx returns live fragments of the given kinds.
func ListIdentityRowsTx(ctx context.Context, tx *sql.Tx, requestID string, kinds []string) ([]IdentityRow, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	marks := make([]string, 0, len(kinds))
	args := []any{requestID}
	for _, k := range kinds {
		marks = append(marks, "?")
		args = append(args, k)
	}
	rows, err := tx.QueryContext(ctx, `SELECT kind, value, fragment_hash FROM request_identities
		WHERE request_id = ? AND kind IN (`+strings.Join(marks, ",")+`) ORDER BY kind, value`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IdentityRow
	for rows.Next() {
		var r IdentityRow
		if err := rows.Scan(&r.Kind, &r.Value, &r.FragmentHash); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteIdentitiesTx removes live fragments of the given kinds (erasure).
func DeleteIdentitiesTx(ctx context.Context, tx *sql.Tx, requestID string, kinds []string) error {
	if len(kinds) == 0 {
		return nil
	}
	marks := make([]string, 0, len(kinds))
	args := []any{requestID}
	for _, k := range kinds {
		marks = append(marks, "?")
		args = append(args, k)
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM request_identities WHERE request_id = ? AND kind IN (`+strings.Join(marks, ",")+`)`, args...)
	return err
}

// InsertErasedEvidenceTx preserves the non-reversible hash of an erased
// fragment. INSERT OR IGNORE keeps repeated erasure idempotent.
func InsertErasedEvidenceTx(ctx context.Context, tx *sql.Tx, requestID string, row ErasureRow) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO erased_identity_evidence
		(request_id, kind, fragment_hash, erased_by_event, erased_at) VALUES (?,?,?,?,?)`,
		requestID, row.Kind, row.FragmentHash, row.ErasedByEvent, row.ErasedAt)
	return err
}

// ListErasedEvidenceTx returns erased-fragment evidence for matching.
func ListErasedEvidenceTx(ctx context.Context, tx *sql.Tx, requestID string) ([]ErasureRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT kind, fragment_hash, erased_by_event, erased_at
		FROM erased_identity_evidence WHERE request_id = ? ORDER BY kind, fragment_hash`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ErasureRow
	for rows.Next() {
		var r ErasureRow
		if err := rows.Scan(&r.Kind, &r.FragmentHash, &r.ErasedByEvent, &r.ErasedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListErasedEvidence returns erased-fragment evidence for the audit summary.
func (s *Store) ListErasedEvidence(ctx context.Context, requestID string) ([]ErasureRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT kind, fragment_hash, erased_by_event, erased_at
		FROM erased_identity_evidence WHERE request_id = ? ORDER BY kind, fragment_hash`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ErasureRow
	for rows.Next() {
		var r ErasureRow
		if err := rows.Scan(&r.Kind, &r.FragmentHash, &r.ErasedByEvent, &r.ErasedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListEventPayloadsTx returns the chain's stored payloads for redaction.
func ListEventPayloadsTx(ctx context.Context, tx *sql.Tx, requestID string) ([]EventRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT seq, channel, payload_json FROM events WHERE request_id = ? ORDER BY seq`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var e EventRow
		if err := rows.Scan(&e.Seq, &e.Channel, &e.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// UpdateEventPayloadTx rewrites the stored display copy of a payload after
// redaction. payload_hash is never touched: it anchors replay integrity.
func UpdateEventPayloadTx(ctx context.Context, tx *sql.Tx, seq int64, payloadJSON string) error {
	_, err := tx.ExecContext(ctx, `UPDATE events SET payload_json = ? WHERE seq = ?`, payloadJSON, seq)
	return err
}

// SetContactErasedTx marks the request's contact data as erased.
func SetContactErasedTx(ctx context.Context, tx *sql.Tx, requestID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE canonical_requests SET contact_erased = 1 WHERE request_id = ?`, requestID)
	return err
}
