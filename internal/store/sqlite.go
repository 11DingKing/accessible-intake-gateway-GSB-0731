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

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS canonical_requests (
    request_id     TEXT PRIMARY KEY,
    person_key     TEXT NOT NULL UNIQUE,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    last_sequence  INTEGER NOT NULL DEFAULT 0
);

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
		return &SubmitOutcome{Result: res, Projection: view}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	env := in.Envelope
	personKey := intake.PersonKey(env.Person)
	requestID := env.CanonicalRequestID
	if requestID == "" && personKey != "" {
		_ = tx.QueryRow(`SELECT request_id FROM canonical_requests WHERE person_key = ?`, personKey).Scan(&requestID)
	}
	if requestID == "" {
		requestID = newRequestID()
		now := time.Now().UTC()
		if personKey == "" {
			personKey = "orphan:" + env.EventID
		}
		if _, err := tx.Exec(`INSERT INTO canonical_requests(request_id, person_key, created_at, updated_at, last_sequence) VALUES(?,?,?,?,0)`,
			requestID, personKey, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			return nil, err
		}
	} else {
		var existingKey string
		err = tx.QueryRow(`SELECT person_key FROM canonical_requests WHERE request_id = ?`, requestID).Scan(&existingKey)
		if errors.Is(err, sql.ErrNoRows) {
			now := time.Now().UTC()
			if personKey == "" {
				personKey = "orphan:" + requestID
			}
			if _, err := tx.Exec(`INSERT INTO canonical_requests(request_id, person_key, created_at, updated_at, last_sequence) VALUES(?,?,?,?,0)`,
				requestID, personKey, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
	}

	now := time.Now().UTC()
	res, err := s.insertEvent(tx, in, requestID, now)
	if err != nil {
		return nil, err
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
	return &SubmitOutcome{Result: res, Projection: view}, nil
}

func (s *Store) insertEvent(tx Executor, in *SubmitInput, requestID string, now time.Time) (*intake.EventResult, error) {
	env := in.Envelope
	personJSON, _ := json.Marshal(env.Person)
	accJSON, _ := json.Marshal(env.Accommodations)
	consentJSON, _ := json.Marshal(env.Consent)
	revJSON, _ := json.Marshal(env.RevocationScopes)
	itemsJSON, _ := json.Marshal(in.ItemResults)
	rawJSON, _ := json.Marshal(in.RawEnvelope)
	var cb sql.NullString
	if env.CallbackWindowMinutes != nil {
		b, _ := json.Marshal(*env.CallbackWindowMinutes)
		cb = sql.NullString{String: string(b), Valid: true}
	}

	res, err := tx.Exec(`INSERT INTO events
(event_id, request_id, channel, source_id, payload_hash, raw_envelope, item_results, status, delivery_status,
 revokes_event_id, revocation_scopes, person, accommodations, consent, callback_minutes, summary, created_at, arrival_order)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		env.EventID, requestID, env.Channel, env.SourceID, in.PayloadHash, string(rawJSON), string(itemsJSON),
		in.Status, intake.DeliveryPending, env.RevokesEventID, string(revJSON), string(personJSON),
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
		channel, sourceID, payloadHash, itemsJSON, status, delivery string
		requestID                                                   string
		seq                                                         int64
		createdAt                                                   string
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
		EventID:            eventID,
		CanonicalRequestID: requestID,
		Channel:            channel,
		SourceID:           sourceID,
		Status:             status,
		ItemResults:        items,
		PayloadHash:        payloadHash,
		Sequence:           seq,
		DeliveryStatus:     delivery,
		CreatedAt:          created,
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
