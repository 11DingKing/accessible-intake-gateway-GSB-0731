// Package store is the SQLite persistence layer for the intake gateway. All
// mutating work happens inside serialized transactions so that concurrent
// submissions of the same canonical request converge onto a single event chain
// and revoked consent is never silently re-exposed by a retry.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/accessible-intake-gateway/internal/model"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// Clock supplies timestamps; overridable so tests can produce stable replays.
type Clock func() time.Time

// Store wraps a SQLite database handle.
type Store struct {
	db    *sql.DB
	clock Clock
}

// Open opens (creating if needed) a SQLite database at dsn and applies the
// schema. dsn may be a file path, ":memory:", or a full "file:..." DSN. The
// connection pool is capped at a single connection so every write serializes —
// the simplest correct way to guarantee a single event chain under concurrency
// with SQLite.
func Open(dsn string) (*Store, error) {
	var conn string
	switch {
	case dsn == ":memory:":
		// A shared in-memory database that survives across the pooled connection.
		conn = "file::memory:?mode=memory&cache=shared"
	case strings.HasPrefix(dsn, "file:"):
		// Caller supplied a complete DSN (used by tests for named memory dbs).
		conn = dsn
	default:
		conn = "file:" + dsn + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
	}
	db, err := sql.Open("sqlite", conn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, clock: time.Now}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// SetClock overrides the timestamp source (used by tests for stable replay).
func (s *Store) SetClock(c Clock) { s.clock = c }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// migrate applies the embedded schema. It is idempotent and safe to re-run.
func (s *Store) migrate() error {
	if _, err := s.db.Exec(schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// Downstream is a pluggable adapter invoked for a valid event before its
// canonical changes are committed. Returning a non-nil error aborts the apply;
// if the error is Transient the event is reported as retriable and nothing is
// persisted beyond a FAILED attempt.
type Downstream func(ev *model.CanonicalEvent) error

// TransientError marks a downstream failure as retriable.
type TransientError struct{ Err error }

func (e *TransientError) Error() string { return e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

func isTransient(err error) bool {
	var t *TransientError
	return errors.As(err, &t)
}

// Apply processes one normalized event with its validation errors and returns
// the per-record result. The whole operation is a transaction: idempotency
// replay, conflict detection, validation rejection, and canonical convergence
// each produce a terminal, replay-stable outcome. down may be nil.
func (s *Store) Apply(ctx context.Context, ev *model.CanonicalEvent, verrs []model.RecordError, down Downstream) (model.RecordResult, error) {
	now := s.clock().UTC().Format(time.RFC3339Nano)

	// Phase 1: idempotency / conflict / validation, inside one transaction.
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return model.RecordResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	attemptNo, err := nextAttemptNo(tx, ev.EventID)
	if err != nil {
		return model.RecordResult{}, err
	}

	// Same event key + same payload -> replay the stored result verbatim.
	if stored, ok, err := lookupIdempotent(tx, ev.EventID, ev.PayloadHash); err != nil {
		return model.RecordResult{}, err
	} else if ok {
		stored.AttemptNo = attemptNo
		stored.Duplicate = true
		if err := recordAttempt(tx, now, attemptNo, ev, stored.CanonicalKey, stored.Status, stored.Errors); err != nil {
			return model.RecordResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.RecordResult{}, err
		}
		committed = true
		return stored, nil
	}

	// Same event key + different payload -> conflict (never overwrites).
	if conflictHash, ok, err := lookupAnyHash(tx, ev.EventID, ev.PayloadHash); err != nil {
		return model.RecordResult{}, err
	} else if ok {
		res := model.RecordResult{
			EventID: ev.EventID, Channel: ev.Channel, Status: model.StatusConflict,
			AttemptNo: attemptNo,
			Errors: []model.RecordError{{
				Field: "eventId", Code: "CONFLICT",
				Message: fmt.Sprintf("eventId %s already recorded with a different payload (hash %s != %s)", ev.EventID, conflictHash, ev.PayloadHash),
			}},
		}
		if err := recordAttempt(tx, now, attemptNo, ev, "", res.Status, res.Errors); err != nil {
			return model.RecordResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.RecordResult{}, err
		}
		committed = true
		return res, nil
	}

	// Validation failures -> terminal rejection (recorded as idempotent so a
	// replay of the same bad payload returns the same result).
	if len(verrs) > 0 {
		res := model.RecordResult{
			EventID: ev.EventID, Channel: ev.Channel, Status: model.StatusRejected,
			AttemptNo: attemptNo, Errors: verrs,
		}
		if err := recordAttempt(tx, now, attemptNo, ev, "", res.Status, verrs); err != nil {
			return model.RecordResult{}, err
		}
		if err := saveIdempotent(tx, ev, res); err != nil {
			return model.RecordResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.RecordResult{}, err
		}
		committed = true
		return res, nil
	}

	// Phase 2: invoke the downstream adapter. A transient failure aborts without
	// persisting canonical changes; a FAILED (retriable) attempt is recorded.
	if down != nil {
		if derr := down(ev); derr != nil {
			_ = tx.Rollback()
			committed = true // prevent deferred double-rollback
			retriable := isTransient(derr)
			status := model.StatusFailed
			res := model.RecordResult{
				EventID: ev.EventID, Channel: ev.Channel, Status: status,
				AttemptNo: attemptNo, Retriable: retriable,
				Errors: []model.RecordError{{Field: "_downstream", Code: "DOWNSTREAM_ERROR", Message: derr.Error()}},
			}
			if !retriable {
				// Non-transient downstream errors are terminal and idempotent.
				res.Status = model.StatusRejected
			}
			if err := s.recordStandaloneAttempt(ctx, now, attemptNo, ev, res.Status, res.Errors, !retriable, res); err != nil {
				return model.RecordResult{}, err
			}
			return res, nil
		}
	}

	// Apply canonical convergence.
	res, err := applyCanonical(tx, now, attemptNo, ev)
	if err != nil {
		return model.RecordResult{}, err
	}
	if err := recordAttempt(tx, now, attemptNo, ev, res.CanonicalKey, res.Status, res.Errors); err != nil {
		return model.RecordResult{}, err
	}
	if err := saveIdempotent(tx, ev, res); err != nil {
		return model.RecordResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.RecordResult{}, err
	}
	committed = true
	return res, nil
}

// recordStandaloneAttempt persists an attempt (and optionally an idempotency
// record) in its own transaction, used after the main transaction rolled back.
func (s *Store) recordStandaloneAttempt(ctx context.Context, now string, attemptNo int, ev *model.CanonicalEvent, status model.Status, errs []model.RecordError, idempotent bool, res model.RecordResult) error {
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := recordAttempt(tx, now, attemptNo, ev, "", status, errs); err != nil {
		return err
	}
	if idempotent {
		if err := saveIdempotent(tx, ev, res); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) beginImmediate(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return tx, nil
}

func nextAttemptNo(tx *sql.Tx, eventID string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM attempt WHERE event_id = ?`, eventID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count attempts: %w", err)
	}
	return n + 1, nil
}

func lookupIdempotent(tx *sql.Tx, eventID, hash string) (model.RecordResult, bool, error) {
	var resultJSON string
	err := tx.QueryRow(`SELECT result_json FROM idempotency WHERE event_id = ? AND payload_hash = ?`, eventID, hash).Scan(&resultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return model.RecordResult{}, false, nil
	}
	if err != nil {
		return model.RecordResult{}, false, fmt.Errorf("lookup idempotency: %w", err)
	}
	var res model.RecordResult
	if err := json.Unmarshal([]byte(resultJSON), &res); err != nil {
		return model.RecordResult{}, false, fmt.Errorf("decode stored result: %w", err)
	}
	return res, true, nil
}

func lookupAnyHash(tx *sql.Tx, eventID, excludeHash string) (string, bool, error) {
	var hash string
	err := tx.QueryRow(`SELECT payload_hash FROM idempotency WHERE event_id = ? AND payload_hash != ? LIMIT 1`, eventID, excludeHash).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("lookup conflict: %w", err)
	}
	return hash, true, nil
}

func recordAttempt(tx *sql.Tx, now string, attemptNo int, ev *model.CanonicalEvent, key string, status model.Status, errs []model.RecordError) error {
	var errJSON sql.NullString
	if len(errs) > 0 {
		b, _ := json.Marshal(errs)
		errJSON = sql.NullString{String: string(b), Valid: true}
	}
	_, err := tx.Exec(`INSERT INTO attempt(event_id, attempt_no, canonical_key, status, payload_hash, errors_json, at) VALUES(?,?,?,?,?,?,?)`,
		ev.EventID, attemptNo, nullStr(key), string(status), ev.PayloadHash, errJSON, now)
	if err != nil {
		return fmt.Errorf("record attempt: %w", err)
	}
	return nil
}

func saveIdempotent(tx *sql.Tx, ev *model.CanonicalEvent, res model.RecordResult) error {
	b, _ := json.Marshal(res)
	_, err := tx.Exec(`INSERT OR IGNORE INTO idempotency(event_id, payload_hash, status, canonical_key, result_json) VALUES(?,?,?,?,?)`,
		ev.EventID, ev.PayloadHash, string(res.Status), nullStr(res.CanonicalKey), string(b))
	if err != nil {
		return fmt.Errorf("save idempotency: %w", err)
	}
	return nil
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func writeAudit(tx *sql.Tx, now, key, eventID, action, detail string) error {
	_, err := tx.Exec(`INSERT INTO audit_entry(canonical_key, event_id, action, detail, at) VALUES(?,?,?,?,?)`,
		key, eventID, action, nullStr(detail), now)
	if err != nil {
		return fmt.Errorf("write audit: %w", err)
	}
	return nil
}

func joinSorted(items []string) string { return strings.Join(items, ",") }
