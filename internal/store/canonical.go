package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/accessible-intake-gateway/internal/model"
)

// applyCanonical converges one valid event into its canonical request within
// the current transaction. It creates the request on first sight, appends to
// the event chain exactly once, merges accommodations, and updates the consent
// ledger with sticky-revocation semantics.
func applyCanonical(tx *sql.Tx, now string, attemptNo int, ev *model.CanonicalEvent) (model.RecordResult, error) {
	res := model.RecordResult{EventID: ev.EventID, Channel: ev.Channel, AttemptNo: attemptNo, CanonicalKey: ev.CanonicalKey}

	requestID, created, err := upsertRequest(tx, now, ev)
	if err != nil {
		return res, err
	}

	// Append to the event chain. The UNIQUE(request_id, event_id) constraint
	// makes concurrent submissions converge to a single chain: the second
	// insert of the same event is ignored, so no duplicate link is created.
	linkRes, err := tx.Exec(`INSERT OR IGNORE INTO event_link(request_id, event_id, channel, kind, source_id_field, source_id, payload_hash) VALUES(?,?,?,?,?,?,?)`,
		requestID, ev.EventID, ev.Channel, string(ev.Kind), nullStr(ev.SourceIDField), nullStr(ev.SourceID), ev.PayloadHash)
	if err != nil {
		return res, fmt.Errorf("append event link: %w", err)
	}
	seq, _ := currentSeq(tx)

	// Merge accommodations (set semantics).
	for _, code := range ev.Accommodations {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO accommodation(request_id, code) VALUES(?,?)`, requestID, code); err != nil {
			return res, fmt.Errorf("merge accommodation: %w", err)
		}
	}

	// Consent grants. A grant never resurrects a scope that is already REVOKED:
	// the ledger only transitions GRANTED for scopes that are absent or already
	// granted, so replays and out-of-order grants cannot re-expose withdrawn
	// consent.
	for _, scope := range ev.ConsentGrants {
		if err := grantConsent(tx, requestID, scope, seq); err != nil {
			return res, err
		}
	}

	// Consent revocations are sticky and terminal for the scope.
	for _, scope := range ev.ConsentRevokes {
		if err := revokeConsent(tx, requestID, scope, seq); err != nil {
			return res, err
		}
	}

	// Re-project the callback contact detail from the effective consent state.
	if err := projectContact(tx, requestID, ev); err != nil {
		return res, err
	}

	if _, err := tx.Exec(`UPDATE canonical_request SET updated_seq = ? WHERE id = ?`, seq, requestID); err != nil {
		return res, fmt.Errorf("bump updated_seq: %w", err)
	}

	action := "EVENT_APPENDED"
	detail := fmt.Sprintf("channel=%s kind=%s", ev.Channel, ev.Kind)
	if n, _ := linkRes.RowsAffected(); n == 0 {
		action = "EVENT_CONVERGED"
		detail += " (chain already contained this event)"
	}
	if created {
		if err := writeAudit(tx, now, ev.CanonicalKey, ev.EventID, "REQUEST_CREATED", fmt.Sprintf("version=%s", ev.CanonicalKey)); err != nil {
			return res, err
		}
	}
	if err := writeAudit(tx, now, ev.CanonicalKey, ev.EventID, action, detail); err != nil {
		return res, err
	}
	if len(ev.ConsentRevokes) > 0 {
		if err := writeAudit(tx, now, ev.CanonicalKey, ev.EventID, "CONSENT_REVOKED", joinSorted(ev.ConsentRevokes)); err != nil {
			return res, err
		}
	}

	res.Status = model.StatusAccepted
	return res, nil
}

// upsertRequest returns the request id for the event's canonical key, creating
// the request if it does not yet exist. INSERT OR IGNORE + re-select makes the
// create safe under concurrency.
func upsertRequest(tx *sql.Tx, now string, ev *model.CanonicalEvent) (int64, bool, error) {
	version := "intake.v1"
	seq, _ := currentSeq(tx)
	r, err := tx.Exec(`INSERT OR IGNORE INTO canonical_request(canonical_key, version, created_seq, updated_seq) VALUES(?,?,?,?)`,
		ev.CanonicalKey, version, seq+1, seq+1)
	if err != nil {
		return 0, false, fmt.Errorf("create request: %w", err)
	}
	var id int64
	err = tx.QueryRow(`SELECT id FROM canonical_request WHERE canonical_key = ?`, ev.CanonicalKey).Scan(&id)
	if err != nil {
		return 0, false, fmt.Errorf("load request id: %w", err)
	}
	created := false
	if n, _ := r.RowsAffected(); n > 0 {
		created = true
	}
	return id, created, nil
}

// currentSeq returns a monotonically increasing sequence sourced from the
// event_link autoincrement counter, giving a stable ordering for projections.
func currentSeq(tx *sql.Tx) (int64, error) {
	var seq sql.NullInt64
	if err := tx.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='event_link'`).Scan(&seq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return seq.Int64, nil
}

func grantConsent(tx *sql.Tx, requestID int64, scope string, seq int64) error {
	var state string
	err := tx.QueryRow(`SELECT state FROM consent WHERE request_id = ? AND scope = ?`, requestID, scope).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec(`INSERT INTO consent(request_id, scope, state, granted_at_seq) VALUES(?,?, 'GRANTED', ?)`, requestID, scope, seq)
		if err != nil {
			return fmt.Errorf("grant consent: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load consent: %w", err)
	}
	// If already REVOKED, do nothing: a grant must not re-expose withdrawn consent.
	return nil
}

func revokeConsent(tx *sql.Tx, requestID int64, scope string, seq int64) error {
	// Upsert into REVOKED state. Once revoked it stays revoked.
	_, err := tx.Exec(`INSERT INTO consent(request_id, scope, state, revoked_at_seq) VALUES(?,?, 'REVOKED', ?)
		ON CONFLICT(request_id, scope) DO UPDATE SET state='REVOKED', revoked_at_seq=excluded.revoked_at_seq`,
		requestID, scope, seq)
	if err != nil {
		return fmt.Errorf("revoke consent: %w", err)
	}
	return nil
}

// projectContact exposes the callback window only while CONTACT_CALLBACK is
// effective. If the scope is revoked, the stored detail is cleared so a retry
// cannot re-expose it.
func projectContact(tx *sql.Tx, requestID int64, ev *model.CanonicalEvent) error {
	if ev.CallbackWindowMinutes != nil {
		_, err := tx.Exec(`INSERT INTO contact_detail(request_id, callback_window_minutes) VALUES(?,?)
			ON CONFLICT(request_id) DO UPDATE SET callback_window_minutes=excluded.callback_window_minutes`,
			requestID, *ev.CallbackWindowMinutes)
		if err != nil {
			return fmt.Errorf("store contact detail: %w", err)
		}
	}
	return nil
}

// GetCanonical loads the fully projected canonical request for a key.
func (s *Store) GetCanonical(canonicalKey string) (model.CanonicalRequest, bool, error) {
	var req model.CanonicalRequest
	var id int64
	err := s.db.QueryRow(`SELECT id, canonical_key, version, created_seq, updated_seq FROM canonical_request WHERE canonical_key = ?`, canonicalKey).
		Scan(&id, &req.CanonicalKey, &req.Version, &req.CreatedSeq, &req.UpdatedSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return req, false, nil
	}
	if err != nil {
		return req, false, fmt.Errorf("load canonical: %w", err)
	}
	req.ID = id

	// Chain.
	rows, err := s.db.Query(`SELECT seq, event_id, channel, kind, COALESCE(source_id_field,''), COALESCE(source_id,'') FROM event_link WHERE request_id = ? ORDER BY seq`, id)
	if err != nil {
		return req, false, fmt.Errorf("load chain: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var l model.ChainLink
		var kind string
		if err := rows.Scan(&l.Seq, &l.EventID, &l.Channel, &kind, &l.SourceIDField, &l.SourceID); err != nil {
			return req, false, err
		}
		l.Kind = model.EventKind(kind)
		req.Chain = append(req.Chain, l)
	}

	// Accommodations.
	req.Accommodations, err = s.stringList(`SELECT code FROM accommodation WHERE request_id = ? ORDER BY code`, id)
	if err != nil {
		return req, false, err
	}

	// Consent projection.
	crows, err := s.db.Query(`SELECT scope, state FROM consent WHERE request_id = ? ORDER BY scope`, id)
	if err != nil {
		return req, false, fmt.Errorf("load consent: %w", err)
	}
	defer crows.Close()
	contactEffective := false
	for crows.Next() {
		var scope, state string
		if err := crows.Scan(&scope, &state); err != nil {
			return req, false, err
		}
		if state == "GRANTED" {
			req.EffectiveConsent = append(req.EffectiveConsent, scope)
			if scope == "CONTACT_CALLBACK" {
				contactEffective = true
			}
		} else {
			req.RevokedConsent = append(req.RevokedConsent, scope)
		}
	}
	sort.Strings(req.EffectiveConsent)
	sort.Strings(req.RevokedConsent)

	// Contact projection: expose the callback window only if CONTACT_CALLBACK is effective.
	req.Contact = model.ContactProjection{Exposed: contactEffective}
	if contactEffective {
		var win sql.NullInt64
		err := s.db.QueryRow(`SELECT callback_window_minutes FROM contact_detail WHERE request_id = ?`, id).Scan(&win)
		if err == nil && win.Valid {
			v := int(win.Int64)
			req.Contact.CallbackWindowMinutes = &v
		}
	}
	return req, true, nil
}

func (s *Store) stringList(query string, args ...interface{}) ([]string, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Attempts returns the append-only attempt log for an event, oldest first.
func (s *Store) Attempts(eventID string) ([]model.Attempt, error) {
	rows, err := s.db.Query(`SELECT event_id, attempt_no, COALESCE(canonical_key,''), status, payload_hash, COALESCE(errors_json,''), at FROM attempt WHERE event_id = ? ORDER BY attempt_no`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load attempts: %w", err)
	}
	defer rows.Close()
	return scanAttempts(rows)
}

// AttemptsForKey returns attempts for every event in a canonical request's chain.
func (s *Store) AttemptsForKey(canonicalKey string) ([]model.Attempt, error) {
	rows, err := s.db.Query(`SELECT a.event_id, a.attempt_no, COALESCE(a.canonical_key,''), a.status, a.payload_hash, COALESCE(a.errors_json,''), a.at
		FROM attempt a WHERE a.canonical_key = ? ORDER BY a.id`, canonicalKey)
	if err != nil {
		return nil, fmt.Errorf("load attempts for key: %w", err)
	}
	defer rows.Close()
	return scanAttempts(rows)
}

func scanAttempts(rows *sql.Rows) ([]model.Attempt, error) {
	var out []model.Attempt
	for rows.Next() {
		var a model.Attempt
		var errsJSON string
		if err := rows.Scan(&a.EventID, &a.AttemptNo, &a.CanonicalKey, (*string)(&a.Status), &a.PayloadHash, &errsJSON, &a.At); err != nil {
			return nil, err
		}
		if errsJSON != "" {
			_ = jsonUnmarshal(errsJSON, &a.Errors)
		}
		out = append(out, a)
	}
	return out, nil
}

// AuditTrail returns the append-only audit entries for a canonical key.
func (s *Store) AuditTrail(canonicalKey string) ([]model.AuditEntry, error) {
	rows, err := s.db.Query(`SELECT seq, canonical_key, event_id, action, COALESCE(detail,''), at FROM audit_entry WHERE canonical_key = ? ORDER BY seq`, canonicalKey)
	if err != nil {
		return nil, fmt.Errorf("load audit: %w", err)
	}
	defer rows.Close()
	var out []model.AuditEntry
	for rows.Next() {
		var e model.AuditEntry
		if err := rows.Scan(&e.Seq, &e.CanonicalKey, &e.EventID, &e.Action, &e.Detail, &e.At); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
