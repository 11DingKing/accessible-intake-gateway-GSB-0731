package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/accessible-intake/gateway/internal/canonical"
)

// ErrNotFound is returned when a requested row does not exist.
var ErrNotFound = errors.New("not found")

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Tx is a single serializable write transaction.
type Tx struct {
	tx *sql.Tx
}

// Begin starts a transaction. The DSN is configured with _txlock=immediate,
// so the underlying statement is BEGIN IMMEDIATE: concurrent writers
// serialize, and two channels reporting the same subject produce exactly one
// event chain.
func (s *Store) Begin(ctx context.Context) (*Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx}, nil
}

// Commit commits the transaction.
func (t *Tx) Commit() error { return t.tx.Commit() }

// Rollback aborts the transaction.
func (t *Tx) Rollback() error { return t.tx.Rollback() }

// nowUTC returns the current time in UTC, centralized for deterministic tests.
func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// NextAttemptNo returns the next attempt number for an event.
func (t *Tx) NextAttemptNo(ctx context.Context, eventID string) (int, error) {
	var n int
	err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(attempt_no),0)+1 FROM event_attempts WHERE event_id=?`, eventID,
	).Scan(&n)
	return n, err
}

// RecordAttempt inserts an attempt row.
func (t *Tx) RecordAttempt(ctx context.Context, eventID string, attemptNo int, reqHash, status string, errs []canonical.ItemResult) error {
	errJSON, _ := json.Marshal(errs)
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO event_attempts(event_id, attempt_no, request_hash, status, errors_json, created_at)
		 VALUES(?,?,?,?,?,?)`,
		eventID, attemptNo, reqHash, status, string(errJSON), nowUTC(),
	)
	return err
}

// EventRow is the stored representation of a committed event.
type EventRow struct {
	EventID          string
	CanonicalID      string
	Channel          string
	SourceID         string
	SourceIDField    string
	PayloadHash      string
	PayloadJSON      string
	IsRevocation     bool
	Revokes          string
	PersonName       string
	PersonPhone      string
	PersonEmail      string
	PersonRef        string
	CallbackWindow   *int
	Sequence         int
	Status           string
	ItemResultsJSON  string
	CreatedAt        string
}

// FindEvent returns the committed event row or ErrNotFound.
func (t *Tx) FindEvent(ctx context.Context, eventID string) (*EventRow, error) {
	row := t.tx.QueryRowContext(ctx, `SELECT
		event_id, canonical_request_id, channel, source_id, source_id_field,
		payload_hash, payload_json, is_revocation, revokes,
		person_name, person_phone, person_email, person_ref,
		callback_window_minutes, sequence, status, item_results_json, created_at
		FROM events WHERE event_id=?`, eventID)
	return scanEvent(row)
}

// FindEventForRead is a read-only variant used outside write transactions.
func (s *Store) FindEvent(ctx context.Context, eventID string) (*EventRow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
		event_id, canonical_request_id, channel, source_id, source_id_field,
		payload_hash, payload_json, is_revocation, revokes,
		person_name, person_phone, person_email, person_ref,
		callback_window_minutes, sequence, status, item_results_json, created_at
		FROM events WHERE event_id=?`, eventID)
	return scanEvent(row)
}

func scanEvent(row *sql.Row) (*EventRow, error) {
	var e EventRow
	var cb sql.NullInt64
	err := row.Scan(
		&e.EventID, &e.CanonicalID, &e.Channel, &e.SourceID, &e.SourceIDField,
		&e.PayloadHash, &e.PayloadJSON, &e.IsRevocation, &e.Revokes,
		&e.PersonName, &e.PersonPhone, &e.PersonEmail, &e.PersonRef,
		&cb, &e.Sequence, &e.Status, &e.ItemResultsJSON, &e.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if cb.Valid {
		v := int(cb.Int64)
		e.CallbackWindow = &v
	}
	return &e, nil
}

// CanonicalRow is the stored canonical request header.
type CanonicalRow struct {
	ID             string
	SubjectKey     string
	Version        string
	CreatedAt      string
	UpdatedAt      string
	FullName       string
	Phone          string
	PhoneDigits    string
	Email          string
	PersonRef      string
	CallbackWindow *int
	Sequence       int
}

const canonicalColumns = `id, subject_key, canonical_version, created_at, updated_at,
		full_name, phone, phone_digits, email, person_ref, callback_window_minutes, sequence`

// FindCanonicalBySubject looks up a chain by its derived subject key.
func (t *Tx) FindCanonicalBySubject(ctx context.Context, key string) (*CanonicalRow, error) {
	row := t.tx.QueryRowContext(ctx, `SELECT `+canonicalColumns+`
		FROM canonical_requests WHERE subject_key=?`, key)
	return scanCanonical(row)
}

// FindCanonicalByIdentity looks up an existing chain whose stored reference
// number, normalized phone, or email matches any non-empty identifier from
// the event. This is what allows an applicant seen at the physical window to
// be linked to a later hotline call or web submission that shares one
// identifier.
func (t *Tx) FindCanonicalByIdentity(ctx context.Context, ref, phoneDigits, email string) (*CanonicalRow, error) {
	var conds []string
	var args []any
	if ref != "" {
		conds = append(conds, "person_ref = ?")
		args = append(args, ref)
	}
	if phoneDigits != "" {
		conds = append(conds, "phone_digits = ?")
		args = append(args, phoneDigits)
	}
	if email != "" {
		conds = append(conds, "email = ?")
		args = append(args, email)
	}
	if len(conds) == 0 {
		return nil, ErrNotFound
	}
	q := `SELECT ` + canonicalColumns + `
		FROM canonical_requests WHERE ` + strings.Join(conds, " OR ") + `
		ORDER BY sequence ASC LIMIT 1`
	row := t.tx.QueryRowContext(ctx, q, args...)
	return scanCanonical(row)
}

// FindCandidatesByIdentity returns ALL canonical chains that match any of the
// provided identifiers, ordered deterministically (oldest chain first). It is
// used by the candidate matching engine to score every possible target.
func (t *Tx) FindCandidatesByIdentity(ctx context.Context, ref, phoneDigits, email, nameNorm string) ([]*CanonicalRow, error) {
	var conds []string
	var args []any
	if ref != "" {
		conds = append(conds, "person_ref = ?")
		args = append(args, ref)
	}
	if phoneDigits != "" {
		conds = append(conds, "phone_digits = ?")
		args = append(args, phoneDigits)
	}
	if email != "" {
		conds = append(conds, "email = ?")
		args = append(args, email)
	}
	if nameNorm != "" {
		conds = append(conds, "lower(full_name) = ?")
		args = append(args, nameNorm)
	}
	if len(conds) == 0 {
		return nil, nil
	}
	q := `SELECT ` + canonicalColumns + `
		FROM canonical_requests WHERE ` + strings.Join(conds, " OR ") + `
		ORDER BY created_at ASC, id ASC`
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*CanonicalRow
	for rows.Next() {
		cr, err := scanCanonicalRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cr)
	}
	return out, rows.Err()
}

// FindCanonicalByID looks up a chain by id (read-only).
func (s *Store) FindCanonicalByID(ctx context.Context, id string) (*CanonicalRow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+canonicalColumns+`
		FROM canonical_requests WHERE id=?`, id)
	return scanCanonical(row)
}

func scanCanonical(row *sql.Row) (*CanonicalRow, error) {
	var c CanonicalRow
	var cb sql.NullInt64
	err := row.Scan(&c.ID, &c.SubjectKey, &c.Version, &c.CreatedAt, &c.UpdatedAt,
		&c.FullName, &c.Phone, &c.PhoneDigits, &c.Email, &c.PersonRef, &cb, &c.Sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if cb.Valid {
		v := int(cb.Int64)
		c.CallbackWindow = &v
	}
	return &c, nil
}

func scanCanonicalRows(rows *sql.Rows) (*CanonicalRow, error) {
	var c CanonicalRow
	var cb sql.NullInt64
	err := rows.Scan(&c.ID, &c.SubjectKey, &c.Version, &c.CreatedAt, &c.UpdatedAt,
		&c.FullName, &c.Phone, &c.PhoneDigits, &c.Email, &c.PersonRef, &cb, &c.Sequence)
	if err != nil {
		return nil, err
	}
	if cb.Valid {
		v := int(cb.Int64)
		c.CallbackWindow = &v
	}
	return &c, nil
}

// CreateCanonical inserts a new canonical request chain.
func (t *Tx) CreateCanonical(ctx context.Context, id, subjectKey, version string) error {
	now := nowUTC()
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO canonical_requests(id, subject_key, canonical_version, created_at, updated_at)
		 VALUES(?,?,?,?,?)`, id, subjectKey, version, now, now)
	return err
}

// CanonicalIDForEvent returns the canonical request id for a committed event.
func (t *Tx) CanonicalIDForEvent(ctx context.Context, eventID string) (string, error) {
	var id string
	err := t.tx.QueryRowContext(ctx,
		`SELECT canonical_request_id FROM events WHERE event_id=?`, eventID,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return id, err
}

// NextSequence returns the next sequence number within a chain.
func (t *Tx) NextSequence(ctx context.Context, canonicalID string) (int, error) {
	var n int
	err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(sequence),0)+1 FROM events WHERE canonical_request_id=?`, canonicalID,
	).Scan(&n)
	return n, err
}

// InsertEvent persists a committed event and returns its sequence.
func (t *Tx) InsertEvent(ctx context.Context, e EventRow) error {
	var cb interface{}
	if e.CallbackWindow != nil {
		cb = *e.CallbackWindow
	}
	rev := 0
	if e.IsRevocation {
		rev = 1
	}
	_, err := t.tx.ExecContext(ctx, `INSERT INTO events(
		event_id, canonical_request_id, channel, source_id, source_id_field,
		payload_hash, payload_json, is_revocation, revokes,
		person_name, person_phone, person_email, person_ref,
		callback_window_minutes, sequence, status, item_results_json, created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.EventID, e.CanonicalID, e.Channel, e.SourceID, e.SourceIDField,
		e.PayloadHash, e.PayloadJSON, rev, e.Revokes,
		e.PersonName, e.PersonPhone, e.PersonEmail, e.PersonRef,
		cb, e.Sequence, e.Status, e.ItemResultsJSON, nowUTC(),
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	// Bump the chain sequence and touch updated_at.
	_, err = t.tx.ExecContext(ctx,
		`UPDATE canonical_requests SET sequence=?, updated_at=? WHERE id=?`,
		e.Sequence, nowUTC(), e.CanonicalID)
	return err
}

// GrantConsent records a GRANT for a scope.
func (t *Tx) GrantConsent(ctx context.Context, canonicalID, eventID string, seq int, scope string) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO consent_records(canonical_request_id, event_id, sequence, scope, action, created_at)
		 VALUES(?,?,?,?,'GRANT',?)`, canonicalID, eventID, seq, scope, nowUTC())
	return err
}

// RevokeConsent records a REVOKE for a scope.
func (t *Tx) RevokeConsent(ctx context.Context, canonicalID, eventID string, seq int, scope string) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO consent_records(canonical_request_id, event_id, sequence, scope, action, created_at)
		 VALUES(?,?,?,?,'REVOKE',?)`, canonicalID, eventID, seq, scope, nowUTC())
	return err
}

// AddAccommodation records a requested accommodation (idempotent per chain).
func (t *Tx) AddAccommodation(ctx context.Context, canonicalID, eventID string, seq int, code string) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO accommodation_records(canonical_request_id, event_id, sequence, code, created_at)
		 VALUES(?,?,?,?,?)`, canonicalID, eventID, seq, code, nowUTC())
	return err
}

// AddAccommodationGuarded records an accommodation only if the accommodations
// field has not been revoked.
func (t *Tx) AddAccommodationGuarded(ctx context.Context, canonicalID, eventID string, seq int, code string, revoked map[string]bool) error {
	if IsFieldRevoked(revoked, "accommodations") {
		return nil
	}
	return t.AddAccommodation(ctx, canonicalID, eventID, seq, code)
}

// UpdateCanonicalPerson updates non-empty person fields and the callback
// window on the canonical header (latest-non-empty-wins), so later events can
// supplement information from another channel. It also maintains the
// phone_digits column used for deterministic candidate matching.
//
// Deprecated: use UpdateCanonicalPersonGuarded which respects revoked fields.
func (t *Tx) UpdateCanonicalPerson(ctx context.Context, canonicalID, name, phone, email, ref string, callback *int) error {
	return t.UpdateCanonicalPersonGuarded(ctx, canonicalID, name, phone, email, ref, callback, nil)
}

// UpdateCanonicalPersonGuarded updates non-empty person fields but skips any
// field present in the revoked set. This prevents a later event or idempotent
// retry from repopulating PII that was cleared by a consent revocation.
func (t *Tx) UpdateCanonicalPersonGuarded(ctx context.Context, canonicalID, name, phone, email, ref string, callback *int, revoked map[string]bool) error {
	if name != "" && !IsFieldRevoked(revoked, "full_name") {
		if _, err := t.tx.ExecContext(ctx, `UPDATE canonical_requests SET full_name=? WHERE id=?`, name, canonicalID); err != nil {
			return err
		}
	}
	if phone != "" && !IsFieldRevoked(revoked, "phone") {
		if _, err := t.tx.ExecContext(ctx, `UPDATE canonical_requests SET phone=?, phone_digits=? WHERE id=?`, phone, digitsOnly(phone), canonicalID); err != nil {
			return err
		}
	}
	if email != "" && !IsFieldRevoked(revoked, "email") {
		if _, err := t.tx.ExecContext(ctx, `UPDATE canonical_requests SET email=? WHERE id=?`, strings.ToLower(strings.TrimSpace(email)), canonicalID); err != nil {
			return err
		}
	}
	if ref != "" {
		if _, err := t.tx.ExecContext(ctx, `UPDATE canonical_requests SET person_ref=? WHERE id=?`, ref, canonicalID); err != nil {
			return err
		}
	}
	if callback != nil {
		if _, err := t.tx.ExecContext(ctx, `UPDATE canonical_requests SET callback_window_minutes=? WHERE id=?`, *callback, canonicalID); err != nil {
			return err
		}
	}
	_, err := t.tx.ExecContext(ctx, `UPDATE canonical_requests SET updated_at=? WHERE id=?`, nowUTC(), canonicalID)
	return err
}

// InsertValidationErrors records per-item errors tied to an event.
func (t *Tx) InsertValidationErrors(ctx context.Context, eventID string, attemptID int64, results []canonical.ItemResult) error {
	for _, r := range results {
		if r.Code == canonical.ItemOK {
			continue
		}
		var aid interface{}
		if attemptID != 0 {
			aid = attemptID
		}
		if _, err := t.tx.ExecContext(ctx,
			`INSERT INTO validation_errors(event_id, attempt_id, item, code, message, created_at)
			 VALUES(?,?,?,?,?,?)`, eventID, aid, r.Item, r.Code, r.Message, nowUTC()); err != nil {
			return err
		}
	}
	return nil
}

// AttemptID returns the last inserted attempt id.
func (t *Tx) LastInsertID(result sql.Result) (int64, error) { return result.LastInsertId() }

// RecordAttemptWithID inserts an attempt and returns its row id.
func (t *Tx) RecordAttemptWithID(ctx context.Context, eventID string, attemptNo int, reqHash, status string, errs []canonical.ItemResult) (int64, error) {
	errJSON, _ := json.Marshal(errs)
	res, err := t.tx.ExecContext(ctx,
		`INSERT INTO event_attempts(event_id, attempt_no, request_hash, status, errors_json, created_at)
		 VALUES(?,?,?,?,?,?)`,
		eventID, attemptNo, reqHash, status, string(errJSON), nowUTC(),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpdateAttemptStatus changes an existing attempt's status.
func (t *Tx) UpdateAttemptStatus(ctx context.Context, eventID string, attemptNo int, status string, errs []canonical.ItemResult) error {
	errJSON, _ := json.Marshal(errs)
	_, err := t.tx.ExecContext(ctx,
		`UPDATE event_attempts SET status=?, errors_json=? WHERE event_id=? AND attempt_no=?`,
		status, string(errJSON), eventID, attemptNo,
	)
	return err
}
