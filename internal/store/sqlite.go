package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/accessible-intake/gateway/internal/intake"
)

const (
	RequestPending   = "pending"
	RequestConfirmed = "confirmed"
)

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS canonical_requests (
    request_id     TEXT PRIMARY KEY,
    person_key     TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'pending',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    last_sequence  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_requests_person_key ON canonical_requests(person_key);

CREATE TABLE IF NOT EXISTS identity_fragments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id   TEXT NOT NULL REFERENCES canonical_requests(request_id),
    event_id     TEXT NOT NULL,
    channel      TEXT NOT NULL,
    first_name   TEXT NOT NULL DEFAULT '',
    last_name    TEXT NOT NULL DEFAULT '',
    date_of_birth TEXT NOT NULL DEFAULT '',
    email        TEXT NOT NULL DEFAULT '',
    phone        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_fragments_request ON identity_fragments(request_id);
CREATE INDEX IF NOT EXISTS idx_fragments_phone ON identity_fragments(phone);
CREATE INDEX IF NOT EXISTS idx_fragments_email ON identity_fragments(email);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fragments_event ON identity_fragments(event_id);

CREATE TABLE IF NOT EXISTS events (
    sequence          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT NOT NULL UNIQUE,
    request_id        TEXT NOT NULL REFERENCES canonical_requests(request_id),
    channel           TEXT NOT NULL,
    source_id         TEXT NOT NULL,
    payload_hash      TEXT NOT NULL,
    raw_envelope      TEXT NOT NULL,
    item_results      TEXT NOT NULL,
    status            TEXT NOT NULL,
    delivery_status   TEXT NOT NULL,
    delivered_hash    TEXT NOT NULL DEFAULT '',
    match_confidence  TEXT NOT NULL DEFAULT 'none',
    match_reason      TEXT NOT NULL DEFAULT '',
    match_score       INTEGER NOT NULL DEFAULT 0,
    match_evidence    TEXT NOT NULL DEFAULT '[]',
    match_conflict    INTEGER NOT NULL DEFAULT 0,
    revokes_event_id  TEXT NOT NULL DEFAULT '',
    revocation_scopes TEXT NOT NULL DEFAULT '[]',
    person            TEXT NOT NULL,
    accommodations    TEXT NOT NULL DEFAULT '[]',
    consent           TEXT NOT NULL DEFAULT '[]',
    callback_minutes  TEXT,
    summary           TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL,
    arrival_order     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_request ON events(request_id, sequence);

CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL,
    attempt_no  INTEGER NOT NULL,
    outcome     TEXT NOT NULL,
    error       TEXT NOT NULL DEFAULT '',
    at          TEXT NOT NULL,
    UNIQUE(event_id, attempt_no)
);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id, attempt_no);
`

var (
	ErrConflict = errors.New("event id conflict: same eventId with different payload")
	ErrNotFound = errors.New("not found")
)

type Adapter interface {
	Deliver(requestID string, view *intake.CanonicalView) error
}

type Executor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

type SubmitInput struct {
	Envelope    *intake.Envelope
	ItemResults []intake.ItemResult
	PayloadHash string
	RawEnvelope json.RawMessage
	Adapter     Adapter
	Status      string
}

type SubmitOutcome struct {
	Result         *intake.EventResult
	Projection     *intake.CanonicalView
	Conflict       bool
	ConflictDetail string
	Match          intake.MatchDecision
}

func newRequestID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "CR-" + hex.EncodeToString(b)
}

func (s *Store) Submit(in *SubmitInput) (*SubmitOutcome, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var existingReq, existingHash, existingStatus, existingDelivery string
	var existingSeq int64
	err = tx.QueryRow(`SELECT request_id, payload_hash, status, delivery_status, sequence FROM events WHERE event_id = ?`, in.Envelope.EventID).Scan(
		&existingReq, &existingHash, &existingStatus, &existingDelivery, &existingSeq,
	)
	if err == nil {
		if existingHash != in.PayloadHash {
			return &SubmitOutcome{Conflict: true, ConflictDetail: "eventId " + in.Envelope.EventID + " already exists with a different payload"}, nil
		}
		res, err := s.loadResult(tx, in.Envelope.EventID)
		if err != nil {
			return nil, err
		}
		view, err := s.projectTx(tx, existingReq)
		if err != nil {
			return nil, err
		}
		match, _ := s.loadMatch(tx, in.Envelope.EventID)
		return &SubmitOutcome{Result: res, Projection: view, Match: match}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	env := in.Envelope
	personKey := intake.PersonKey(env.Person)
	incoming := fragmentFromEnvelope(env)

	requestID, match, err := s.resolveRequestID(tx, env, personKey, incoming)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	res, err := s.insertEvent(tx, in, requestID, now, match)
	if err != nil {
		return nil, err
	}
	if err := s.insertFragment(tx, requestID, env.EventID, env.Channel, incoming); err != nil {
		return nil, err
	}

	// Once a complete identity (name+DOB) is attached, confirm the request and
	// consolidate any other request (pending fragment-only or a previously
	// confirmed duplicate for the same person) into it so only one canonical
	// chain remains.
	if personKey != "" {
		requestID, err = s.confirmAndConsolidate(tx, requestID, personKey)
		if err != nil {
			return nil, err
		}
		requestID, err = s.consolidatePendingCandidates(tx, requestID, incoming)
		if err != nil {
			return nil, err
		}
	}

	view, err := s.projectTx(tx, requestID)
	if err != nil {
		return nil, err
	}
	res.CanonicalRequestID = requestID

	deliveryStatus := intake.DeliveryDelivered
	var deliveryErr string
	outcome := in.Status
	if in.Adapter != nil {
		if err := in.Adapter.Deliver(requestID, view); err != nil {
			deliveryStatus = intake.DeliveryPending
			deliveryErr = err.Error()
			outcome = intake.StatusAdapterFailure
		}
	}
	res.DeliveryStatus = deliveryStatus
	res.Status = outcome
	if _, err := tx.Exec(`UPDATE events SET status = ?, delivery_status = ? WHERE event_id = ?`, outcome, deliveryStatus, env.EventID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO attempts(event_id, attempt_no, outcome, error, at) VALUES(?,?,?,?,?)`,
		env.EventID, 1, outcome, deliveryErr, now.Format(time.RFC3339Nano)); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE canonical_requests SET updated_at = ?, last_sequence = ? WHERE request_id = ?`,
		now.Format(time.RFC3339Nano), res.Sequence, requestID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	res.Attempts = []intake.AttemptRecord{{AttemptNo: 1, Outcome: outcome, Error: deliveryErr, At: now}}
	return &SubmitOutcome{Result: res, Projection: view, Match: match}, nil
}

// resolveRequestID finds or creates the canonical request for an incoming event
// using, in order: explicit canonicalRequestId, exact person-key, strong
// identifier (phone/email) match, or a new pending request.
func (s *Store) resolveRequestID(tx *sql.Tx, env *intake.Envelope, personKey string, incoming intake.IdentityFragment) (string, intake.MatchDecision, error) {
	match := intake.MatchDecision{Confidence: intake.ConfidenceNone, Reason: intake.MatchReasonNoMatch}

	if env.CanonicalRequestID != "" {
		var status string
		err := tx.QueryRow(`SELECT status FROM canonical_requests WHERE request_id = ?`, env.CanonicalRequestID).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			now := time.Now().UTC()
			pk := personKey
			if pk == "" {
				pk = "orphan:" + env.CanonicalRequestID
			}
			if _, err := tx.Exec(`INSERT INTO canonical_requests(request_id, person_key, status, created_at, updated_at) VALUES(?,?,?,?,?)`,
				env.CanonicalRequestID, pk, RequestPending, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
				return "", match, err
			}
			status = RequestPending
		} else if err != nil {
			return "", match, err
		}
		_ = status
		match.Confidence = intake.ConfidenceExact
		match.Reason = intake.MatchReasonCrossChannelLink
		match.RequestID = env.CanonicalRequestID
		match.Score = 200
		return env.CanonicalRequestID, match, nil
	}

	if personKey != "" {
		var rid string
		err := tx.QueryRow(`SELECT request_id FROM canonical_requests WHERE person_key = ?`, personKey).Scan(&rid)
		if err == nil {
			match = intake.MatchDecision{RequestID: rid, Confidence: intake.ConfidenceExact, Reason: intake.MatchReasonExactIdentity, Score: 100}
			return rid, match, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", match, err
		}
	}

	// Strong-identifier match against stored fragments (phone/email). Phone and
	// email are looked up across all requests; the deterministic matcher picks
	// the best candidate. Single-connection write serialization guarantees two
	// concurrent claimants cannot each create a separate chain.
	rid, match, err := s.findBestCandidate(tx, incoming)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", match, err
	}
	if rid != "" {
		match.RequestID = rid
		return rid, match, nil
	}

	// No confident match: create a pending request. The fragment is recorded
	// after insertion, and a later complete-identity event will consolidate.
	rid = newRequestID()
	now := time.Now().UTC()
	pk := personKey
	if pk == "" {
		pk = "pending:" + rid
	}
	status := RequestPending
	if personKey != "" {
		status = RequestConfirmed
	}
	if _, err := tx.Exec(`INSERT INTO canonical_requests(request_id, person_key, status, created_at, updated_at) VALUES(?,?,?,?,?)`,
		rid, pk, status, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		return "", match, err
	}
	match.Confidence = intake.ConfidenceNone
	match.Reason = intake.MatchReasonNoMatch
	match.RequestID = rid
	return rid, match, nil
}

func (s *Store) findBestCandidate(tx *sql.Tx, in intake.IdentityFragment) (string, intake.MatchDecision, error) {
	phone := normalize(in.Phone)
	email := normalizeEmail(in.Email)

	var args []any
	var where string
	switch {
	case phone != "" && email != "":
		where = "WHERE phone = ? OR email = ?"
		args = []any{phone, email}
	case phone != "":
		where = "WHERE phone = ?"
		args = []any{phone}
	case email != "":
		where = "WHERE email = ?"
		args = []any{email}
	default:
		return "", intake.MatchDecision{}, sql.ErrNoRows
	}

	rows, err := tx.Query(`SELECT DISTINCT request_id FROM identity_fragments `+where+` ORDER BY request_id`, args...)
	if err != nil {
		return "", intake.MatchDecision{}, err
	}
	var rids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return "", intake.MatchDecision{}, err
		}
		rids = append(rids, id)
	}
	rows.Close()
	if len(rids) == 0 {
		return "", intake.MatchDecision{}, sql.ErrNoRows
	}

	cands := make([]intake.IdentityCandidate, 0, len(rids))
	for _, rid := range rids {
		c, err := s.loadCandidates(tx, rid)
		if err != nil {
			return "", intake.MatchDecision{}, err
		}
		cands = append(cands, c...)
	}
	decision := intake.MatchFragment(in, cands)
	if !decision.AutoMerge() {
		return "", intake.MatchDecision{}, sql.ErrNoRows
	}
	return decision.RequestID, decision, nil
}

// confirmAndConsolidate marks the canonical request for personKey as confirmed
// and merges every other request sharing that person key into the earliest one
// so that only a single canonical event chain ever exists. It returns the
// surviving request ID.
func (s *Store) confirmAndConsolidate(tx *sql.Tx, requestID, personKey string) (string, error) {
	rows, err := tx.Query(`SELECT request_id FROM canonical_requests WHERE person_key = ? ORDER BY created_at ASC, request_id ASC`, personKey)
	if err != nil {
		return requestID, err
	}
	var all []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return requestID, err
		}
		all = append(all, id)
	}
	rows.Close()

	target := requestID
	for _, id := range all {
		if id < target {
			target = id
		}
	}
	if len(all) == 0 {
		target = requestID
	}

	if _, err := tx.Exec(`UPDATE canonical_requests SET status = ?, person_key = ? WHERE request_id = ?`, RequestConfirmed, personKey, target); err != nil {
		return target, err
	}
	for _, dup := range all {
		if dup == target {
			continue
		}
		if _, err := tx.Exec(`UPDATE events SET request_id = ? WHERE request_id = ?`, target, dup); err != nil {
			return target, err
		}
		if _, err := tx.Exec(`UPDATE identity_fragments SET request_id = ? WHERE request_id = ?`, target, dup); err != nil {
			return target, err
		}
		if _, err := tx.Exec(`DELETE FROM canonical_requests WHERE request_id = ?`, dup); err != nil {
			return target, err
		}
	}
	return target, nil
}

// consolidatePendingCandidates merges every other pending request whose
// identity fragments match the incoming full-identity fragment with high
// confidence into the target request. Only auto-merges exact/high matches;
// conflicting medium/low candidates remain recorded as evidence but do not
// create a second chain because the target is already confirmed.
func (s *Store) consolidatePendingCandidates(tx *sql.Tx, target string, incoming intake.IdentityFragment) (string, error) {
	rows, err := tx.Query(`SELECT request_id FROM canonical_requests WHERE status = ? AND request_id <> ? ORDER BY created_at ASC, request_id ASC`, RequestPending, target)
	if err != nil {
		return target, err
	}
	var pending []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return target, err
		}
		pending = append(pending, id)
	}
	rows.Close()

	for _, pid := range pending {
		cands, err := s.loadCandidates(tx, pid)
		if err != nil {
			return target, err
		}
		decision := intake.MatchFragment(incoming, cands)
		if decision.AutoMerge() && !decision.Conflict {
			if _, err := tx.Exec(`UPDATE events SET request_id = ? WHERE request_id = ?`, target, pid); err != nil {
				return target, err
			}
			if _, err := tx.Exec(`UPDATE identity_fragments SET request_id = ? WHERE request_id = ?`, target, pid); err != nil {
				return target, err
			}
			if _, err := tx.Exec(`DELETE FROM canonical_requests WHERE request_id = ?`, pid); err != nil {
				return target, err
			}
		}
	}
	return target, nil
}

func fragmentFromEnvelope(env *intake.Envelope) intake.IdentityFragment {
	return intake.IdentityFragment{
		FirstName:   env.Person.FirstName,
		LastName:    env.Person.LastName,
		DateOfBirth: env.Person.DateOfBirth,
		Email:       env.Person.Email,
		Phone:       env.Person.Phone,
		Channel:     env.Channel,
		EventID:     env.EventID,
	}
}

func normalize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r >= '0' && r <= '9' {
			out = append(out, r)
		}
	}
	return string(out)
}

func normalizeEmail(s string) string {
	return lowerTrim(s)
}

func lowerTrim(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		out = append(out, r)
	}
	return string(out)
}

func (s *Store) insertFragment(tx Executor, requestID, eventID, channel string, f intake.IdentityFragment) error {
	_, err := tx.Exec(`INSERT INTO identity_fragments(request_id, event_id, channel, first_name, last_name, date_of_birth, email, phone) VALUES(?,?,?,?,?,?,?,?)`,
		requestID, eventID, channel, f.FirstName, f.LastName, f.DateOfBirth, normalizeEmail(f.Email), normalize(f.Phone))
	return err
}

func (s *Store) insertEvent(tx Executor, in *SubmitInput, requestID string, now time.Time, match intake.MatchDecision) (*intake.EventResult, error) {
	env := in.Envelope
	personJSON, _ := json.Marshal(env.Person)
	accJSON, _ := json.Marshal(env.Accommodations)
	consentJSON, _ := json.Marshal(env.Consent)
	revJSON, _ := json.Marshal(env.RevocationScopes)
	itemsJSON, _ := json.Marshal(in.ItemResults)
	rawJSON, _ := json.Marshal(in.RawEnvelope)
	evidenceJSON, _ := json.Marshal(match.Evidence)
	var cb sql.NullString
	if env.CallbackWindowMinutes != nil {
		b, _ := json.Marshal(*env.CallbackWindowMinutes)
		cb = sql.NullString{String: string(b), Valid: true}
	}
	conflictFlag := 0
	if match.Conflict {
		conflictFlag = 1
	}

	res, err := tx.Exec(`INSERT INTO events
(event_id, request_id, channel, source_id, payload_hash, raw_envelope, item_results, status, delivery_status,
 match_confidence, match_reason, match_score, match_evidence, match_conflict,
 revokes_event_id, revocation_scopes, person, accommodations, consent, callback_minutes, summary, created_at, arrival_order)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		env.EventID, requestID, env.Channel, env.SourceID, in.PayloadHash, string(rawJSON), string(itemsJSON),
		in.Status, intake.DeliveryPending, match.Confidence, match.Reason, match.Score, string(evidenceJSON), conflictFlag,
		env.RevokesEventID, string(revJSON), string(personJSON),
		string(accJSON), string(consentJSON), cb, env.Summary, now.Format(time.RFC3339Nano), now.UnixNano())
	if err != nil {
		return nil, err
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return &intake.EventResult{
		EventID:            env.EventID,
		CanonicalRequestID: requestID,
		Channel:            env.Channel,
		SourceID:           env.SourceID,
		Status:             in.Status,
		ItemResults:        in.ItemResults,
		PayloadHash:        in.PayloadHash,
		Sequence:           seq,
		DeliveryStatus:     intake.DeliveryPending,
		CreatedAt:          now,
	}, nil
}

func (s *Store) loadMatch(q Executor, eventID string) (intake.MatchDecision, error) {
	var confidence, reason, evidenceJSON string
	var score int
	var conflict int
	err := q.QueryRow(`SELECT match_confidence, match_reason, match_score, match_evidence, match_conflict FROM events WHERE event_id = ?`, eventID).Scan(
		&confidence, &reason, &score, &evidenceJSON, &conflict)
	if err != nil {
		return intake.MatchDecision{}, err
	}
	var ev []intake.MatchEvidence
	_ = json.Unmarshal([]byte(evidenceJSON), &ev)
	return intake.MatchDecision{Confidence: confidence, Reason: reason, Score: score, Evidence: ev, Conflict: conflict == 1}, nil
}

func (s *Store) RetryDelivery(eventID string, adapter Adapter) (*intake.EventResult, *intake.CanonicalView, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	var requestID string
	err = tx.QueryRow(`SELECT request_id FROM events WHERE event_id = ?`, eventID).Scan(&requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}

	view, err := s.projectTx(tx, requestID)
	if err != nil {
		return nil, nil, err
	}

	var nextNo int
	_ = tx.QueryRow(`SELECT COALESCE(MAX(attempt_no),0) FROM attempts WHERE event_id = ?`, eventID).Scan(&nextNo)
	nextNo++
	now := time.Now().UTC()
	outcome := intake.DeliveryDelivered
	var deliveryErr string
	if adapter != nil {
		if err := adapter.Deliver(requestID, view); err != nil {
			outcome = intake.DeliveryFailed
			deliveryErr = err.Error()
		}
	}
	if outcome == intake.DeliveryDelivered {
		if _, err := tx.Exec(`UPDATE events SET delivery_status = ?, delivered_hash = ? WHERE event_id = ?`, intake.DeliveryDelivered, view.ProjectionHash, eventID); err != nil {
			return nil, nil, err
		}
	} else {
		if _, err := tx.Exec(`UPDATE events SET delivery_status = ? WHERE event_id = ?`, intake.DeliveryPending, eventID); err != nil {
			return nil, nil, err
		}
	}
	if _, err := tx.Exec(`INSERT INTO attempts(event_id, attempt_no, outcome, error, at) VALUES(?,?,?,?,?)`,
		eventID, nextNo, outcome, deliveryErr, now.Format(time.RFC3339Nano)); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	res, err := s.loadResult(s.db, eventID)
	if err != nil {
		return nil, nil, err
	}
	return res, view, nil
}

func (s *Store) EventByID(eventID string) (*intake.EventResult, error) {
	return s.loadResult(s.db, eventID)
}

func (s *Store) loadResult(q Executor, eventID string) (*intake.EventResult, error) {
	var (
		channel, sourceID, payloadHash, itemsJSON, status, delivery, createdAt string
		requestID                                                              string
		seq                                                                    int64
	)
	err := q.QueryRow(`SELECT request_id, channel, source_id, payload_hash, item_results, status, delivery_status, sequence, created_at FROM events WHERE event_id = ?`, eventID).Scan(
		&requestID, &channel, &sourceID, &payloadHash, &itemsJSON, &status, &delivery, &seq, &createdAt,
	)
	if err != nil {
		return nil, err
	}
	var items []intake.ItemResult
	_ = json.Unmarshal([]byte(itemsJSON), &items)
	created, _ := time.Parse(time.RFC3339Nano, createdAt)
	res := &intake.EventResult{
		EventID: eventID, CanonicalRequestID: requestID, Channel: channel, SourceID: sourceID,
		Status: status, ItemResults: items, PayloadHash: payloadHash, Sequence: seq, DeliveryStatus: delivery,
		CreatedAt: created,
	}
	attempts, err := s.attemptsFor(q, eventID)
	if err != nil {
		return nil, err
	}
	res.Attempts = attempts
	return res, nil
}

func (s *Store) attemptsFor(q Executor, eventID string) ([]intake.AttemptRecord, error) {
	rows, err := q.Query(`SELECT attempt_no, outcome, error, at FROM attempts WHERE event_id = ? ORDER BY attempt_no`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []intake.AttemptRecord
	for rows.Next() {
		var no int
		var outcome, errstr, at string
		if err := rows.Scan(&no, &outcome, &errstr, &at); err != nil {
			return nil, err
		}
		t, _ := time.Parse(time.RFC3339Nano, at)
		out = append(out, intake.AttemptRecord{AttemptNo: no, Outcome: outcome, Error: errstr, At: t})
	}
	return out, rows.Err()
}

func (s *Store) loadCandidates(tx Executor, requestID string) ([]intake.IdentityCandidate, error) {
	rows, err := tx.Query(`SELECT request_id, first_name, last_name, date_of_birth, email, phone, channel, event_id FROM identity_fragments WHERE request_id = ?`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cand := intake.IdentityCandidate{RequestID: requestID}
	for rows.Next() {
		var f intake.IdentityFragment
		var rid string
		if err := rows.Scan(&rid, &f.FirstName, &f.LastName, &f.DateOfBirth, &f.Email, &f.Phone, &f.Channel, &f.EventID); err != nil {
			return nil, err
		}
		cand.Fragments = append(cand.Fragments, f)
	}
	return []intake.IdentityCandidate{cand}, rows.Err()
}

func (s *Store) loadAppliedEvents(q Executor, requestID string) ([]*intake.AppliedEvent, error) {
	rows, err := q.Query(`SELECT sequence, event_id, channel, source_id, person, accommodations, consent, callback_minutes, revokes_event_id, revocation_scopes, summary, arrival_order, created_at FROM events WHERE request_id = ? ORDER BY sequence`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*intake.AppliedEvent
	for rows.Next() {
		var (
			seq, arrival                     int64
			eventID, channel, sourceID       string
			personJSON, accJSON, consentJSON string
			revokes                          string
			revJSON                          string
			summary, createdAt               string
			cbJSON                           sql.NullString
		)
		if err := rows.Scan(&seq, &eventID, &channel, &sourceID, &personJSON, &accJSON, &consentJSON, &cbJSON, &revokes, &revJSON, &summary, &arrival, &createdAt); err != nil {
			return nil, err
		}
		ev := &intake.AppliedEvent{
			Sequence: seq, EventID: eventID, Channel: channel, SourceID: sourceID,
			RevokesEventID: revokes, Summary: summary, ArrivalOrder: arrival,
		}
		_ = json.Unmarshal([]byte(personJSON), &ev.Person)
		_ = json.Unmarshal([]byte(accJSON), &ev.Accommodations)
		_ = json.Unmarshal([]byte(consentJSON), &ev.Consent)
		_ = json.Unmarshal([]byte(revJSON), &ev.RevocationScopes)
		if cbJSON.Valid {
			var n int
			if json.Unmarshal([]byte(cbJSON.String), &n) == nil {
				ev.CallbackWindowMinutes = &n
			}
		}
		ev.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) projectTx(tx *sql.Tx, requestID string) (*intake.CanonicalView, error) {
	events, err := s.loadAppliedEvents(tx, requestID)
	if err != nil {
		return nil, err
	}
	return intake.Project(requestID, events), nil
}

func (s *Store) Project(requestID string) (*intake.CanonicalView, error) {
	var exists int
	err := s.db.QueryRow(`SELECT 1 FROM canonical_requests WHERE request_id = ?`, requestID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	events, err := s.loadAppliedEvents(s.db, requestID)
	if err != nil {
		return nil, err
	}
	return intake.Project(requestID, events), nil
}

func (s *Store) EventChain(requestID string) ([]*intake.EventResult, error) {
	rows, err := s.db.Query(`SELECT event_id FROM events WHERE request_id = ? ORDER BY sequence`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]*intake.EventResult, 0, len(ids))
	for _, id := range ids {
		r, err := s.loadResult(s.db, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

type MatchRecord struct {
	EventID    string                 `json:"eventId"`
	RequestID  string                 `json:"requestId"`
	Confidence string                 `json:"confidence"`
	Reason     string                 `json:"reason"`
	Score      int                    `json:"score"`
	Conflict   bool                   `json:"conflict"`
	Evidence   []intake.MatchEvidence `json:"evidence"`
}

func (s *Store) MatchForEvent(eventID string) (MatchRecord, error) {
	var m MatchRecord
	var confidence, reason, evidenceJSON string
	var score, conflict int
	err := s.db.QueryRow(`SELECT event_id, request_id, match_confidence, match_reason, match_score, match_evidence, match_conflict FROM events WHERE event_id = ?`, eventID).Scan(
		&m.EventID, &m.RequestID, &confidence, &reason, &score, &evidenceJSON, &conflict)
	if errors.Is(err, sql.ErrNoRows) {
		return m, ErrNotFound
	}
	if err != nil {
		return m, err
	}
	m.Confidence = confidence
	m.Reason = reason
	m.Score = score
	m.Conflict = conflict == 1
	_ = json.Unmarshal([]byte(evidenceJSON), &m.Evidence)
	return m, nil
}

func (s *Store) PendingRequests() ([]string, error) {
	rows, err := s.db.Query(`SELECT request_id FROM canonical_requests WHERE status = ? ORDER BY created_at`, RequestPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type AuditSummary struct {
	RequestID      string    `json:"requestId"`
	EventCount     int       `json:"eventCount"`
	LastSequence   int64     `json:"lastSequence"`
	ProjectionHash string    `json:"projectionHash"`
	Chain          []string  `json:"chain"`
	GeneratedAt    time.Time `json:"generatedAt"`
}

func (s *Store) AuditSummary(requestID string) (*AuditSummary, error) {
	view, err := s.Project(requestID)
	if err != nil {
		return nil, err
	}
	return &AuditSummary{
		RequestID:      requestID,
		EventCount:     len(view.EventChain),
		LastSequence:   view.Sequence,
		ProjectionHash: view.ProjectionHash,
		Chain:          view.EventChain,
		GeneratedAt:    time.Now().UTC(),
	}, nil
}
