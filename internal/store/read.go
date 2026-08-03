package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/accessible-intake/gateway/internal/canonical"
)

// SnapshotOptions controls how a canonical snapshot is projected.
type SnapshotOptions struct {
	// RedactContacts, when true, omits phone/email unless CONTACT_CALLBACK is
	// currently granted. This is always enforced for API responses.
	RedactContacts bool
}

// BuildSnapshot reconstructs the current canonical state by replaying the
// event chain in sequence order. The reconstruction is deterministic: given
// the same rows it always yields the same snapshot.
func (s *Store) BuildSnapshot(ctx context.Context, canonicalID string, opts SnapshotOptions) (canonical.CanonicalSnapshot, error) {
	var snap canonical.CanonicalSnapshot

	row := s.db.QueryRowContext(ctx, `SELECT id, subject_key, canonical_version, created_at, updated_at, sequence
		FROM canonical_requests WHERE id=?`, canonicalID)
	var createdAt, updatedAt string
	if err := row.Scan(&snap.ID, &snap.SubjectKey, &snap.CanonicalVersion, &createdAt, &updatedAt, &snap.Sequence); err != nil {
		if err == sql.ErrNoRows {
			return snap, ErrNotFound
		}
		return snap, err
	}
	snap.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	snap.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)

	rows, err := s.db.QueryContext(ctx, `SELECT event_id, channel, source_id, source_id_field, payload_hash,
		is_revocation, status, created_at, sequence
		FROM events WHERE canonical_request_id=? ORDER BY sequence ASC`, canonicalID)
	if err != nil {
		return snap, err
	}
	defer rows.Close()

	type evtHead struct {
		eventID, channel, sourceID, field, hash, status, createdAt string
		isRev, hasPerson bool
		seq int
	}
	var evts []evtHead
	latestPerson := canonical.Person{}
	var latestCallback *int
	for rows.Next() {
		var h evtHead
		var isRev int
		if err := rows.Scan(&h.eventID, &h.channel, &h.sourceID, &h.field, &h.hash,
			&isRev, &h.status, &h.createdAt, &h.seq); err != nil {
			return snap, err
		}
		h.isRev = isRev != 0
		evts = append(evts, h)
	}
	if err := rows.Err(); err != nil {
		return snap, err
	}

	// Pull person/callback from each non-revocation event; latest non-empty wins.
	for _, h := range evts {
		if h.isRev {
			continue
		}
		var name, phone, email, ref string
		var cb sql.NullInt64
		err := s.db.QueryRowContext(ctx,
			`SELECT person_name, person_phone, person_email, person_ref, callback_window_minutes
			 FROM events WHERE event_id=?`, h.eventID,
		).Scan(&name, &phone, &email, &ref, &cb)
		if err != nil {
			return snap, err
		}
		if name != "" {
			latestPerson.FullName = name
		}
		if phone != "" {
			latestPerson.Phone = phone
		}
		if email != "" {
			latestPerson.Email = email
		}
		if ref != "" {
			latestPerson.ReferenceNumber = ref
		}
		if cb.Valid {
			v := int(cb.Int64)
			latestCallback = &v
		}
	}

	// Effective consent = latest action per scope.
	consentRows, err := s.db.QueryContext(ctx,
		`SELECT scope, action FROM consent_records WHERE canonical_request_id=? ORDER BY sequence ASC, id ASC`,
		canonicalID)
	if err != nil {
		return snap, err
	}
	defer consentRows.Close()
	consentState := map[string]string{}
	for consentRows.Next() {
		var scope, action string
		if err := consentRows.Scan(&scope, &action); err != nil {
			return snap, err
		}
		consentState[scope] = action
	}
	if err := consentRows.Err(); err != nil {
		return snap, err
	}
	for scope, action := range consentState {
		if action == "GRANT" {
			snap.Consent = append(snap.Consent, scope)
		}
	}
	sort.Strings(snap.Consent)

	// Accommodations (valid codes only; unknown codes were never stored).
	accRows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT code FROM accommodation_records WHERE canonical_request_id=? ORDER BY code ASC`,
		canonicalID)
	if err != nil {
		return snap, err
	}
	defer accRows.Close()
	for accRows.Next() {
		var code string
		if err := accRows.Scan(&code); err != nil {
			return snap, err
		}
		snap.Accommodations = append(snap.Accommodations, code)
	}
	if err := accRows.Err(); err != nil {
		return snap, err
	}

	// Sources in sequence order, deduped by (channel, sourceId).
	seen := map[string]struct{}{}
	for _, h := range evts {
		key := h.channel + "|" + h.sourceID
		if _, ok := seen[key]; ok || h.sourceID == "" {
			continue
		}
		seen[key] = struct{}{}
		snap.Sources = append(snap.Sources, canonical.SourceRef{
			Channel:  h.channel,
			SourceID: h.sourceID,
			Field:    h.field,
		})
	}

	snap.Person = latestPerson
	snap.CallbackWindowMinutes = latestCallback

	// Load revoked fields. These were cleared at revocation time, but we
	// re-check here as a safety net so that any stale or manually-added
	// data cannot cause a revoked field to reappear.
	revoked, err := s.RevokedFields(ctx, canonicalID)
	if err != nil {
		return snap, err
	}
	for f := range revoked {
		snap.RevokedFields = append(snap.RevokedFields, f)
	}
	sort.Strings(snap.RevokedFields)
	if revoked["phone"] {
		snap.Person.Phone = ""
	}
	if revoked["email"] {
		snap.Person.Email = ""
	}
	if revoked["full_name"] {
		snap.Person.FullName = ""
	}
	if revoked["accommodations"] {
		snap.Accommodations = nil
	}

	// Contact masking: without current CONTACT_CALLBACK consent, contact
	// details are never projected into a response, even on idempotent retry.
	if opts.RedactContacts {
		granted := false
		for _, c := range snap.Consent {
			if c == "CONTACT_CALLBACK" {
				granted = true
				break
			}
		}
		if !granted {
			snap.Person.Phone = ""
			snap.Person.Email = ""
			snap.ContactMasked = true
		}
	}

	return snap, nil
}

// EventItemResults returns the per-item results stored for an event.
func (s *Store) EventItemResults(ctx context.Context, eventID string) ([]canonical.ItemResult, error) {
	var raw string
	err := s.db.QueryRowContext(ctx,
		`SELECT item_results_json FROM events WHERE event_id=?`, eventID,
	).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var results []canonical.ItemResult
	if err := jsonUnmarshal(raw, &results); err != nil {
		return nil, err
	}
	if results == nil {
		results = []canonical.ItemResult{}
	}
	return results, nil
}

// Attempts returns all attempts for an event, oldest first.
func (s *Store) Attempts(ctx context.Context, eventID string) ([]canonical.Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_id, attempt_no, request_hash, status, errors_json, created_at
		 FROM event_attempts WHERE event_id=? ORDER BY attempt_no ASC`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []canonical.Attempt
	for rows.Next() {
		var a canonical.Attempt
		var errsRaw, created string
		if err := rows.Scan(&a.ID, &a.EventID, &a.AttemptNo, &a.RequestHash, &a.Status, &errsRaw, &created); err != nil {
			return nil, err
		}
		_ = jsonUnmarshal(errsRaw, &a.Errors)
		a.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, a)
	}
	return out, rows.Err()
}

// AuditSummary builds the minimal replayable audit summary for a chain.
func (s *Store) AuditSummary(ctx context.Context, canonicalID string) (canonical.AuditSummary, error) {
	snap, err := s.BuildSnapshot(ctx, canonicalID, SnapshotOptions{RedactContacts: false})
	if err != nil {
		return canonical.AuditSummary{}, err
	}
	sum := canonical.AuditSummary{
		CanonicalRequestID: snap.ID,
		SubjectKey:         snap.SubjectKey,
		LatestSequence:     snap.Sequence,
		CurrentConsent:     snap.Consent,
		Accommodations:     snap.Accommodations,
		RevokedFields:      snap.RevokedFields,
		Sources:            snap.Sources,
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT event_id, channel, source_id, payload_hash, status, created_at, sequence
		 FROM events WHERE canonical_request_id=? ORDER BY sequence ASC`, canonicalID)
	if err != nil {
		return sum, err
	}
	defer rows.Close()
	for rows.Next() {
		var ea canonical.EventAudit
		var created string
		if err := rows.Scan(&ea.EventID, &ea.Channel, &ea.SourceID, &ea.PayloadHash, &ea.Status, &created, &ea.Sequence); err != nil {
			return sum, err
		}
		ea.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		sum.Events = append(sum.Events, ea)
	}
	if err := rows.Err(); err != nil {
		return sum, err
	}
	sum.EventCount = len(sum.Events)

	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM event_attempts WHERE event_id IN (SELECT event_id FROM events WHERE canonical_request_id=?)`,
		canonicalID,
	).Scan(&sum.AttemptCount); err != nil {
		return sum, err
	}
	return sum, nil
}

// All attempts across the chain (for audit detail).
func (s *Store) AttemptsForCanonical(ctx context.Context, canonicalID string) ([]canonical.Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT a.id, a.event_id, a.attempt_no, a.request_hash, a.status, a.errors_json, a.created_at
		 FROM event_attempts a
		 JOIN events e ON e.event_id = a.event_id
		 WHERE e.canonical_request_id=?
		 ORDER BY a.id ASC`, canonicalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []canonical.Attempt
	for rows.Next() {
		var a canonical.Attempt
		var errsRaw, created string
		if err := rows.Scan(&a.ID, &a.EventID, &a.AttemptNo, &a.RequestHash, &a.Status, &errsRaw, &created); err != nil {
			return nil, err
		}
		_ = jsonUnmarshal(errsRaw, &a.Errors)
		a.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, a)
	}
	return out, rows.Err()
}

func jsonUnmarshal(raw string, v any) error {
	if raw == "" {
		return nil
	}
	return json.Unmarshal([]byte(raw), v)
}
