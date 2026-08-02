package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// schemaVersion is bumped whenever the DDL changes; migrations are
// idempotent so opening a fresh or existing database always converges.
const schemaVersion = 1

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

// Migrate applies the schema. It is safe to run any number of times.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	var applied int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, schemaVersion).Scan(&applied)
	if err != nil {
		return fmt.Errorf("check schema version: %w", err)
	}
	if applied > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, schemaDDL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (?)`, schemaVersion); err != nil {
		return fmt.Errorf("record schema version: %w", err)
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
	RequestID  string
	PersonKey  string
	PersonJSON string
	StateJSON  string
	EventCount int
	CreatedAt  string
	UpdatedAt  string
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

const requestCols = `request_id, person_key, person_json, state_json, event_count, created_at, updated_at`

func scanRequest(row interface{ Scan(...any) error }) (*RequestRow, error) {
	var r RequestRow
	if err := row.Scan(&r.RequestID, &r.PersonKey, &r.PersonJSON, &r.StateJSON, &r.EventCount, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
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
	res, err := tx.ExecContext(ctx, `INSERT INTO canonical_requests (`+requestCols+`) VALUES (?,?,?,?,?,?,?) ON CONFLICT(person_key) DO NOTHING`,
		r.RequestID, r.PersonKey, r.PersonJSON, r.StateJSON, r.EventCount, r.CreatedAt, r.UpdatedAt)
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
