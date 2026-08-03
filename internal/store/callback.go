package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/accessible-intake/gateway/internal/canonical"
)

// InsertCallback persists the hotline-specific projection of an event.
func (t *Tx) InsertCallback(ctx context.Context, cb canonical.CallbackRecord) error {
	var linked interface{}
	if cb.LinkedAt != nil {
		linked = cb.LinkedAt.UTC().Format(time.RFC3339Nano)
	}
	var win interface{}
	if cb.CallbackWindow != nil {
		win = *cb.CallbackWindow
	}
	contactExposed := 0
	if cb.ContactExposed {
		contactExposed = 1
	}
	consentPending := 0
	if cb.ConsentPending {
		consentPending = 1
	}
	_, err := t.tx.ExecContext(ctx, `INSERT INTO callback_events(
		event_id, canonical_request_id, channel, source_id, source_id_field,
		callback_window_minutes, window_unit, window_status, match_status,
		confidence, contact_exposed, consent_pending, created_at, linked_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		cb.EventID, cb.CanonicalRequestID, cb.Channel, cb.SourceID, cb.SourceIDField,
		win, cb.WindowUnit, cb.WindowStatus, cb.MatchStatus,
		cb.Confidence, contactExposed, consentPending,
		cb.CreatedAt.UTC().Format(time.RFC3339Nano), linked)
	return err
}

// InsertMatchCandidate persists one evaluated candidate for an event.
func (t *Tx) InsertMatchCandidate(ctx context.Context, eventID string, mc canonical.MatchCandidate) error {
	reasons, _ := json.Marshal(mc.Reasons)
	conflicts, _ := json.Marshal(mc.Conflicts)
	selected := 0
	if mc.Selected {
		selected = 1
	}
	_, err := t.tx.ExecContext(ctx, `INSERT INTO match_candidates(
		event_id, canonical_request_id, rank, selected, confidence, score,
		reasons_json, conflicts_json, created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		eventID, mc.CanonicalRequestID, mc.Rank, selected, mc.Confidence,
		mc.Score, string(reasons), string(conflicts), nowUTC())
	return err
}

// CallbackRow is the stored representation of a callback event.
type CallbackRow struct {
	EventID            string
	CanonicalRequestID string
	Channel            string
	SourceID           string
	SourceIDField      string
	CallbackWindow     *int
	WindowUnit         string
	WindowStatus       string
	MatchStatus        string
	Confidence         string
	ContactExposed     bool
	ConsentPending     bool
	CreatedAt          string
	LinkedAt           sql.NullString
}

// FindCallback retrieves a callback event by event id.
func (s *Store) FindCallback(ctx context.Context, eventID string) (*CallbackRow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
		event_id, canonical_request_id, channel, source_id, source_id_field,
		callback_window_minutes, window_unit, window_status, match_status,
		confidence, contact_exposed, consent_pending, created_at, linked_at
		FROM callback_events WHERE event_id=?`, eventID)
	return scanCallback(row)
}

// FindCallbacksByCanonical returns all callbacks for a chain.
func (s *Store) FindCallbacksByCanonical(ctx context.Context, canonicalID string) ([]*CallbackRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		event_id, canonical_request_id, channel, source_id, source_id_field,
		callback_window_minutes, window_unit, window_status, match_status,
		confidence, contact_exposed, consent_pending, created_at, linked_at
		FROM callback_events WHERE canonical_request_id=? ORDER BY created_at ASC`, canonicalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*CallbackRow
	for rows.Next() {
		cr, err := scanCallbackRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cr)
	}
	return out, rows.Err()
}

func scanCallback(row *sql.Row) (*CallbackRow, error) {
	cr := &CallbackRow{}
	var cb sql.NullInt64
	var contactExp, consentPen int
	err := row.Scan(
		&cr.EventID, &cr.CanonicalRequestID, &cr.Channel, &cr.SourceID, &cr.SourceIDField,
		&cb, &cr.WindowUnit, &cr.WindowStatus, &cr.MatchStatus,
		&cr.Confidence, &contactExp, &consentPen, &cr.CreatedAt, &cr.LinkedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if cb.Valid {
		v := int(cb.Int64)
		cr.CallbackWindow = &v
	}
	cr.ContactExposed = contactExp != 0
	cr.ConsentPending = consentPen != 0
	return cr, nil
}

func scanCallbackRows(rows *sql.Rows) (*CallbackRow, error) {
	cr := &CallbackRow{}
	var cb sql.NullInt64
	var contactExp, consentPen int
	err := rows.Scan(
		&cr.EventID, &cr.CanonicalRequestID, &cr.Channel, &cr.SourceID, &cr.SourceIDField,
		&cb, &cr.WindowUnit, &cr.WindowStatus, &cr.MatchStatus,
		&cr.Confidence, &contactExp, &consentPen, &cr.CreatedAt, &cr.LinkedAt,
	)
	if err != nil {
		return nil, err
	}
	if cb.Valid {
		v := int(cb.Int64)
		cr.CallbackWindow = &v
	}
	cr.ContactExposed = contactExp != 0
	cr.ConsentPending = consentPen != 0
	return cr, nil
}

// MatchCandidateRow is the stored representation of a match candidate.
type MatchCandidateRow struct {
	EventID            string
	CanonicalRequestID string
	Rank               int
	Selected           bool
	Confidence         string
	Score              int
	ReasonsJSON        string
	ConflictsJSON      string
}

// FindMatchCandidates returns the evaluated candidates for an event, in rank
// order.
func (s *Store) FindMatchCandidates(ctx context.Context, eventID string) ([]canonical.MatchCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		event_id, canonical_request_id, rank, selected, confidence, score,
		reasons_json, conflicts_json
		FROM match_candidates WHERE event_id=? ORDER BY rank ASC`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []canonical.MatchCandidate
	for rows.Next() {
		var mcr MatchCandidateRow
		var selected int
		if err := rows.Scan(&mcr.EventID, &mcr.CanonicalRequestID, &mcr.Rank,
			&selected, &mcr.Confidence, &mcr.Score,
			&mcr.ReasonsJSON, &mcr.ConflictsJSON); err != nil {
			return nil, err
		}
		mcr.Selected = selected != 0
		var reasons, conflicts []canonical.MatchEvidence
		_ = json.Unmarshal([]byte(mcr.ReasonsJSON), &reasons)
		_ = json.Unmarshal([]byte(mcr.ConflictsJSON), &conflicts)
		if reasons == nil {
			reasons = []canonical.MatchEvidence{}
		}
		if conflicts == nil {
			conflicts = []canonical.MatchEvidence{}
		}
		out = append(out, canonical.MatchCandidate{
			CanonicalRequestID: mcr.CanonicalRequestID,
			Rank:               mcr.Rank,
			Selected:           mcr.Selected,
			Confidence:         mcr.Confidence,
			Score:              mcr.Score,
			Reasons:            reasons,
			Conflicts:          conflicts,
		})
	}
	return out, rows.Err()
}
