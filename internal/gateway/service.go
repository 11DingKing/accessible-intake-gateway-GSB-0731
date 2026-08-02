package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// contactScope is the consent scope whose revocation triggers contact
// erasure. The name comes verbatim from the channel contract.
const contactScope = "CONTACT_CALLBACK"

// contactKinds are the identity fragment kinds erased on contact revocation.
var contactKinds = []string{identityPhone, identityEmail}

// Service normalizes channel envelopes into canonical requests.
type Service struct {
	store *Store
	reg   *Registry
	now   func() time.Time

	// transientHook simulates a transient downstream failure. It is only
	// used by tests; it is never reachable through the HTTP API.
	transientHook func(eventID string, raw []byte) error
}

// NewService builds a Service on an already-migrated store.
func NewService(store *Store, reg *Registry) *Service {
	return &Service{store: store, reg: reg, now: func() time.Time { return time.Now().UTC() }}
}

// SetNow overrides the clock (tests only).
func (s *Service) SetNow(now func() time.Time) { s.now = now }

// SetTransientHook installs a hook evaluated before processing each record;
// a non-nil return takes the transient-failure path. Tests only.
func (s *Service) SetTransientHook(hook func(eventID string, raw []byte) error) {
	s.transientHook = hook
}

// ProcessEnvelope normalizes one raw envelope. It returns the per-record
// result plus the HTTP status the single-record endpoint should use.
//
// Transaction semantics per record:
//   - one SQLite transaction per record; the record either lands completely
//     (event + folded state + attempt) or not at all;
//   - an accepted event is applied exactly once: same eventId + same payload
//     replays the stored original result without mutating state;
//   - same eventId + different payload is a conflict and changes nothing;
//   - validation failures persist only an attempt row, so a corrected retry
//     with the same eventId is processed on its own merits;
//   - transient failures roll everything back and are marked retryable.
func (s *Service) ProcessEnvelope(ctx context.Context, raw []byte) (RecordResult, int) {
	hash, err := PayloadHash(raw)
	if err != nil {
		return RecordResult{
			Status: StatusFailed,
			Errors: []FieldError{{Code: CodeInvalidJSON, Message: "payload is not valid JSON: " + err.Error()}},
		}, http.StatusBadRequest
	}

	env, verrs := parseEnvelope(s.reg, raw)
	result := RecordResult{EventID: env.EventID}

	if s.transientHook != nil {
		if herr := s.transientHook(env.EventID, raw); herr != nil {
			return s.failTransient(ctx, env.EventID, hash, herr)
		}
	}
	if len(verrs) > 0 {
		result.Status = StatusFailed
		result.Errors = verrs
		s.logAttemptBestEffort(ctx, &AttemptRow{
			EventID: env.EventID, PayloadHash: hash, Outcome: AttemptValidationFailed,
			DetailJSON: string(mustMarshal(map[string]any{"errors": verrs})), AttemptedAt: s.stamp(),
		})
		return result, http.StatusUnprocessableEntity
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return s.failTransient(ctx, env.EventID, hash, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	failTx := func(err error) (RecordResult, int) {
		_ = tx.Rollback()
		committed = true // suppress deferred rollback
		return s.failTransient(ctx, env.EventID, hash, err)
	}

	existing, err := GetEventTx(ctx, tx, env.EventID)
	if err != nil {
		return failTx(err)
	}
	if existing != nil {
		if existing.PayloadHash == hash {
			var stored RecordResult
			if err := json.Unmarshal([]byte(existing.ResultJSON), &stored); err != nil {
				return failTx(fmt.Errorf("stored result unreadable: %w", err))
			}
			stored.IdempotentReplay = true
			if err := InsertAttemptTx(ctx, tx, &AttemptRow{
				EventID: env.EventID, PayloadHash: hash, Outcome: AttemptReplayed,
				RequestID: existing.RequestID, AttemptedAt: s.stamp(),
			}); err != nil {
				return failTx(err)
			}
			if err := tx.Commit(); err != nil {
				return s.failTransient(ctx, env.EventID, hash, err)
			}
			committed = true
			return stored, http.StatusOK
		}
		result.Status = StatusConflict
		result.RequestID = existing.RequestID
		result.Errors = []FieldError{{
			Code:    CodePayloadConflict,
			Field:   "eventId",
			Message: fmt.Sprintf("eventId %q was already accepted with a different payload (stored hash %s)", env.EventID, shortHash(existing.PayloadHash)),
		}}
		if err := InsertAttemptTx(ctx, tx, &AttemptRow{
			EventID: env.EventID, PayloadHash: hash, Outcome: AttemptConflict,
			DetailJSON: string(mustMarshal(map[string]any{"errors": result.Errors})),
			RequestID:  existing.RequestID, AttemptedAt: s.stamp(),
		}); err != nil {
			return failTx(err)
		}
		if err := tx.Commit(); err != nil {
			return s.failTransient(ctx, env.EventID, hash, err)
		}
		committed = true
		return result, http.StatusConflict
	}

	var req *RequestRow
	var state *RequestState
	var appliedScopes []string
	var match *MatchEvidence

	if env.EventType == EventTypeRevocation {
		target, err := GetEventTx(ctx, tx, env.Revokes)
		if err != nil {
			return failTx(err)
		}
		if target == nil {
			// Out-of-order revocation: the grant may arrive later, so the
			// caller may retry; nothing but the attempt is persisted.
			result.Status = StatusFailed
			result.Retryable = true
			result.Errors = []FieldError{{
				Code:    CodeRevokesUnknownEvent,
				Field:   "revokes",
				Message: fmt.Sprintf("event %q has not been accepted yet; retry after it lands", env.Revokes),
			}}
			if err := InsertAttemptTx(ctx, tx, &AttemptRow{
				EventID: env.EventID, PayloadHash: hash, Outcome: AttemptValidationFailed,
				DetailJSON: string(mustMarshal(map[string]any{"errors": result.Errors})), AttemptedAt: s.stamp(),
			}); err != nil {
				return failTx(err)
			}
			if err := tx.Commit(); err != nil {
				return s.failTransient(ctx, env.EventID, hash, err)
			}
			committed = true
			return result, http.StatusUnprocessableEntity
		}
		req, err = GetRequestByIDTx(ctx, tx, target.RequestID)
		if err != nil || req == nil {
			return failTx(fmt.Errorf("request for event %q: %w", env.Revokes, err))
		}
		state, err = parseState(req.StateJSON)
		if err != nil {
			return failTx(err)
		}
		appliedScopes = state.applyRevocation(env)
		// Contact erasure fires only when this revocation removed the last
		// live CONTACT_CALLBACK grant; other scopes never touch contact data,
		// and a still-effective scope is never over-cleared.
		if contains(appliedScopes, contactScope) && !contains(state.EffectiveConsent(), contactScope) {
			if err := s.eraseContactTx(ctx, tx, req, env.EventID); err != nil {
				return failTx(err)
			}
			req.ContactErased = true
		}
	} else {
		req, match, err = s.resolveRequestTx(ctx, tx, env)
		if err != nil {
			return failTx(err)
		}
		state, err = parseState(req.StateJSON)
		if err != nil {
			return failTx(err)
		}
		state.applyIntake(env)
		// After erasure, contact details re-accumulate only with a fresh
		// CONTACT_CALLBACK grant folded by this very event.
		allowContact := !req.ContactErased || contains(state.EffectiveConsent(), contactScope)
		if err := accumulatePersonTx(ctx, tx, req, env.Person, allowContact); err != nil {
			return failTx(err)
		}
	}

	now := s.stamp()
	if err := UpdateRequestStateTx(ctx, tx, req.RequestID, string(mustMarshal(state)), req.EventCount+1, now); err != nil {
		return failTx(err)
	}
	// On an erased request without a fresh grant, the stored display copy of
	// the payload keeps raw contact out of the database. payload_hash always
	// covers the original incoming bytes, so replay integrity is unchanged.
	storedPayload := string(mustCanonical(raw))
	if env.EventType == EventTypeIntake && req.ContactErased && !contains(state.EffectiveConsent(), contactScope) {
		if contract, ok := s.reg.Channel(env.Channel); ok {
			storedPayload = s.redactPayloadContact(storedPayload, contract.PersonField)
		}
	}
	seq, err := InsertEventTx(ctx, tx, &EventRow{
		EventID: env.EventID, RequestID: req.RequestID, EventType: env.EventType,
		Channel: env.Channel, CorrelationID: env.CorrelationID,
		PayloadJSON: storedPayload, PayloadHash: hash, AppliedAt: now,
	})
	if err != nil {
		return failTx(err)
	}

	result.Status = StatusAccepted
	result.RequestID = req.RequestID
	result.Seq = seq
	result.AppliedScopes = appliedScopes
	result.EffectiveConsent = state.EffectiveConsent()
	result.Match = match
	if err := SetEventResultTx(ctx, tx, seq, string(mustMarshal(result))); err != nil {
		return failTx(err)
	}
	if err := InsertAttemptTx(ctx, tx, &AttemptRow{
		EventID: env.EventID, PayloadHash: hash, Outcome: AttemptAccepted,
		RequestID: req.RequestID, AttemptedAt: now,
	}); err != nil {
		return failTx(err)
	}
	if err := tx.Commit(); err != nil {
		return s.failTransient(ctx, env.EventID, hash, err)
	}
	committed = true
	return result, http.StatusCreated
}

// failTransient rolls the record back entirely, logs the attempt as
// transient and tells the caller to retry.
func (s *Service) failTransient(ctx context.Context, eventID, hash string, cause error) (RecordResult, int) {
	result := RecordResult{
		EventID:   eventID,
		Status:    StatusFailed,
		Retryable: true,
		Errors:    []FieldError{{Code: CodeTransientFailure, Message: "transient failure, safe to retry: " + cause.Error()}},
	}
	s.logAttemptBestEffort(ctx, &AttemptRow{
		EventID: eventID, PayloadHash: hash, Outcome: AttemptTransientFailure,
		DetailJSON: string(mustMarshal(map[string]any{"errors": result.Errors})), AttemptedAt: s.stamp(),
	})
	return result, http.StatusServiceUnavailable
}

// logAttemptBestEffort records an attempt outside any transaction; failures
// here never change the record's outcome.
func (s *Service) logAttemptBestEffort(ctx context.Context, a *AttemptRow) {
	_ = s.store.ExecOutsideTx(ctx, `INSERT INTO attempts (event_id, payload_hash, outcome, detail_json, request_id, attempted_at) VALUES (?,?,?,?,?,?)`,
		a.EventID, a.PayloadHash, a.Outcome, a.DetailJSON, a.RequestID, a.AttemptedAt)
}

// eraseContactTx purges raw contact data for one request inside the
// revocation's transaction and preserves only non-reversible evidence:
//   - person_json loses phone/email (name and idNumber are not contact
//     channels and are kept);
//   - live phone/email identity fragments move to erased_identity_evidence
//     (hash + erasing event + timestamp), so later events can still match by
//     hash without the database retaining the raw values;
//   - stored payload copies on the chain are redacted the same way;
//   - payload_hash columns are never touched, keeping replay integrity.
//
// The operation is idempotent: already-erased rows simply match nothing.
func (s *Service) eraseContactTx(ctx context.Context, tx *sql.Tx, req *RequestRow, erasedByEvent string) error {
	now := s.stamp()
	rows, err := ListIdentityRowsTx(ctx, tx, req.RequestID, contactKinds)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := InsertErasedEvidenceTx(ctx, tx, req.RequestID, ErasureRow{
			Kind: row.Kind, FragmentHash: row.FragmentHash,
			ErasedByEvent: erasedByEvent, ErasedAt: now,
		}); err != nil {
			return err
		}
	}
	if err := DeleteIdentitiesTx(ctx, tx, req.RequestID, contactKinds); err != nil {
		return err
	}
	if req.PersonJSON != "" {
		var p Person
		if err := json.Unmarshal([]byte(req.PersonJSON), &p); err != nil {
			return fmt.Errorf("stored person unreadable: %w", err)
		}
		p.Phone = ""
		p.Email = ""
		if err := UpdateRequestPersonTx(ctx, tx, req.RequestID, string(mustMarshal(&p))); err != nil {
			return err
		}
	}
	events, err := ListEventPayloadsTx(ctx, tx, req.RequestID)
	if err != nil {
		return err
	}
	for _, e := range events {
		contract, ok := s.reg.Channel(e.Channel)
		if !ok {
			continue // revocation events carry no person payload
		}
		redacted := s.redactPayloadContact(e.PayloadJSON, contract.PersonField)
		if redacted != e.PayloadJSON {
			if err := UpdateEventPayloadTx(ctx, tx, e.Seq, redacted); err != nil {
				return err
			}
		}
	}
	return SetContactErasedTx(ctx, tx, req.RequestID)
}

// redactPayloadContact replaces phone/email values inside the payload's
// person object with a "REDACTED#<fragmentHash>" marker: the hash keeps the
// evidence legal and non-reversible while the raw contact leaves the
// database. Other fields are untouched.
func (s *Service) redactPayloadContact(payloadJSON, personField string) string {
	if personField == "" {
		return payloadJSON
	}
	dec := json.NewDecoder(strings.NewReader(payloadJSON))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return payloadJSON
	}
	personRaw, ok := doc[personField]
	if !ok {
		return payloadJSON
	}
	person, ok := personRaw.(map[string]any)
	if !ok {
		return payloadJSON
	}
	changed := false
	for _, field := range []struct {
		name string
		kind string
	}{
		{"phone", identityPhone},
		{"email", identityEmail},
	} {
		raw, ok := person[field.name].(string)
		if !ok || raw == "" || strings.HasPrefix(raw, "REDACTED#") {
			continue
		}
		normalized := raw
		if field.kind == identityPhone {
			normalized = digitsOnly(raw)
		} else {
			normalized = strings.ToLower(strings.TrimSpace(raw))
		}
		person[field.name] = "REDACTED#" + fragmentHash(field.kind, normalized)
		changed = true
	}
	if !changed {
		return payloadJSON
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return payloadJSON
	}
	return string(out)
}

// --- Read projections -------------------------------------------------
// PersonView is the minimized applicant view exposed over HTTP.
type PersonView struct {
	Name string `json:"name"`
}

// ContactView is only exposed while CONTACT_CALLBACK consent is effective.
type ContactView struct {
	Phone string `json:"phone,omitempty"`
	Email string `json:"email,omitempty"`
}

// RequestView is the canonical normalized result for one request.
type RequestView struct {
	RequestID             string                  `json:"requestId"`
	CanonicalVersion      string                  `json:"canonicalVersion"`
	Person                *PersonView             `json:"person,omitempty"`
	Contact               *ContactView            `json:"contact,omitempty"`
	Channels              map[string]*ChannelLink `json:"channels"`
	Accommodations        []string                `json:"accommodations"`
	ConsentEffective      []string                `json:"consentEffective"`
	ConsentRevoked        []string                `json:"consentRevoked"`
	CallbackWindowMinutes *int64                  `json:"callbackWindowMinutes,omitempty"`
	CallbackWindowKind    string                  `json:"callbackWindowKind,omitempty"`
	EventCount            int                     `json:"eventCount"`
	CreatedAt             string                  `json:"createdAt"`
	UpdatedAt             string                  `json:"updatedAt"`
}

// GetRequest returns the consent-filtered projection of a request.
// Revoking CONTACT_CALLBACK removes contact details from the projection,
// and replays of older events can never bring them back.
func (s *Service) GetRequest(ctx context.Context, requestID string) (*RequestView, error) {
	req, err := s.store.GetRequest(ctx, requestID)
	if err != nil || req == nil {
		return nil, err
	}
	state, err := parseState(req.StateJSON)
	if err != nil {
		return nil, err
	}
	for _, link := range state.Channels {
		link.CallbackWindowKind = callbackWindowKind(link.CallbackWindowMinutes)
	}
	view := &RequestView{
		RequestID:             req.RequestID,
		CanonicalVersion:      state.CanonicalVersion,
		Channels:              state.Channels,
		Accommodations:        state.Accommodations,
		ConsentEffective:      state.EffectiveConsent(),
		ConsentRevoked:        state.RevokedScopes(),
		CallbackWindowMinutes: state.CallbackWindowMinutes,
		CallbackWindowKind:    callbackWindowKind(state.CallbackWindowMinutes),
		EventCount:            req.EventCount,
		CreatedAt:             req.CreatedAt,
		UpdatedAt:             req.UpdatedAt,
	}
	if req.PersonJSON != "" {
		var p Person
		if err := json.Unmarshal([]byte(req.PersonJSON), &p); err != nil {
			return nil, err
		}
		view.Person = &PersonView{Name: p.Name}
		if contains(state.EffectiveConsent(), "CONTACT_CALLBACK") {
			view.Contact = &ContactView{Phone: p.Phone, Email: p.Email}
		}
	}
	return view, nil
}

// EventView is one entry of the replayable event chain.
type EventView struct {
	Seq           int64           `json:"seq"`
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	Channel       string          `json:"channel,omitempty"`
	CorrelationID string          `json:"correlationId,omitempty"`
	PayloadHash   string          `json:"payloadHash"`
	AppliedAt     string          `json:"appliedAt"`
	Payload       json.RawMessage `json:"payload"`
	Match         *MatchEvidence  `json:"match,omitempty"`
}

// ListEvents returns the request's full event chain, stable in seq order.
func (s *Service) ListEvents(ctx context.Context, requestID string) ([]EventView, bool, error) {
	req, err := s.store.GetRequest(ctx, requestID)
	if err != nil || req == nil {
		return nil, req != nil, err
	}
	rows, err := s.store.ListEvents(ctx, requestID)
	if err != nil {
		return nil, true, err
	}
	out := make([]EventView, 0, len(rows))
	for _, r := range rows {
		var match *MatchEvidence
		if r.ResultJSON != "" {
			var stored RecordResult
			if err := json.Unmarshal([]byte(r.ResultJSON), &stored); err == nil {
				match = stored.Match
			}
		}
		out = append(out, EventView{
			Seq: r.Seq, EventID: r.EventID, EventType: r.EventType, Channel: r.Channel,
			CorrelationID: r.CorrelationID, PayloadHash: r.PayloadHash, AppliedAt: r.AppliedAt,
			Payload: json.RawMessage(r.PayloadJSON), Match: match,
		})
	}
	return out, true, nil
}

// ErasureEvidenceView is the audit-facing, non-reversible proof that one
// contact fragment was erased.
type ErasureEvidenceView struct {
	Kind          string `json:"kind"`
	FragmentHash  string `json:"fragmentHash"`
	ErasedByEvent string `json:"erasedByEvent"`
	ErasedAt      string `json:"erasedAt"`
}

// AuditView is the minimal audit summary of a request.
type AuditView struct {
	RequestID             string                `json:"requestId"`
	CanonicalVersion      string                `json:"canonicalVersion"`
	PersonKey             string                `json:"personKey"`
	EventCount            int                   `json:"eventCount"`
	FirstSeq              int64                 `json:"firstSeq,omitempty"`
	LastSeq               int64                 `json:"lastSeq,omitempty"`
	Channels              []string              `json:"channels"`
	Correlations          map[string]string     `json:"correlations"`
	Accommodations        []string              `json:"accommodations"`
	ConsentEffective      []string              `json:"consentEffective"`
	ConsentRevoked        []string              `json:"consentRevoked"`
	CallbackWindowMinutes *int64                `json:"callbackWindowMinutes,omitempty"`
	CallbackWindowKind    string                `json:"callbackWindowKind,omitempty"`
	ErasedIdentities      []ErasureEvidenceView `json:"erasedIdentities,omitempty"`
	StateHash             string                `json:"stateHash"`
	CreatedAt             string                `json:"createdAt"`
	UpdatedAt             string                `json:"updatedAt"`
}

// GetAudit returns the minimal audit summary; StateHash pins the exact
// folded state so replays can be compared byte for byte.
func (s *Service) GetAudit(ctx context.Context, requestID string) (*AuditView, error) {
	req, err := s.store.GetRequest(ctx, requestID)
	if err != nil || req == nil {
		return nil, err
	}
	state, err := parseState(req.StateJSON)
	if err != nil {
		return nil, err
	}
	rows, err := s.store.ListEvents(ctx, requestID)
	if err != nil {
		return nil, err
	}
	audit := &AuditView{
		RequestID:             req.RequestID,
		CanonicalVersion:      state.CanonicalVersion,
		PersonKey:             req.PersonKey,
		EventCount:            req.EventCount,
		Channels:              []string{},
		Correlations:          map[string]string{},
		Accommodations:        state.Accommodations,
		ConsentEffective:      state.EffectiveConsent(),
		ConsentRevoked:        state.RevokedScopes(),
		CallbackWindowMinutes: state.CallbackWindowMinutes,
		CallbackWindowKind:    callbackWindowKind(state.CallbackWindowMinutes),
		StateHash:             stateHash(req.StateJSON),
		CreatedAt:             req.CreatedAt,
		UpdatedAt:             req.UpdatedAt,
	}
	for ch, link := range state.Channels {
		audit.Channels = append(audit.Channels, ch)
		audit.Correlations[ch] = link.CorrelationID
	}
	sort.Strings(audit.Channels)
	if len(rows) > 0 {
		audit.FirstSeq = rows[0].Seq
		audit.LastSeq = rows[len(rows)-1].Seq
	}
	erased, err := s.store.ListErasedEvidence(ctx, requestID)
	if err != nil {
		return nil, err
	}
	for _, row := range erased {
		audit.ErasedIdentities = append(audit.ErasedIdentities, ErasureEvidenceView{
			Kind: row.Kind, FragmentHash: row.FragmentHash,
			ErasedByEvent: row.ErasedByEvent, ErasedAt: row.ErasedAt,
		})
	}
	return audit, nil
}

// AttemptView is one logged submission attempt.
type AttemptView struct {
	ID          int64           `json:"id"`
	EventID     string          `json:"eventId"`
	PayloadHash string          `json:"payloadHash"`
	Outcome     string          `json:"outcome"`
	Detail      json.RawMessage `json:"detail,omitempty"`
	RequestID   string          `json:"requestId,omitempty"`
	AttemptedAt string          `json:"attemptedAt"`
}

// ListAttempts returns every attempt touching the request's event chain.
func (s *Service) ListAttempts(ctx context.Context, requestID string) ([]AttemptView, bool, error) {
	req, err := s.store.GetRequest(ctx, requestID)
	if err != nil || req == nil {
		return nil, req != nil, err
	}
	rows, err := s.store.ListAttemptsForRequest(ctx, requestID)
	if err != nil {
		return nil, true, err
	}
	out := make([]AttemptView, 0, len(rows))
	for _, r := range rows {
		var detail json.RawMessage
		if r.DetailJSON != "" {
			detail = json.RawMessage(r.DetailJSON)
		}
		out = append(out, AttemptView{
			ID: r.ID, EventID: r.EventID, PayloadHash: r.PayloadHash, Outcome: r.Outcome,
			Detail: detail, RequestID: r.RequestID, AttemptedAt: r.AttemptedAt,
		})
	}
	return out, true, nil
}

// --- helpers ----------------------------------------------------------

func (s *Service) stamp() string { return s.now().Format(time.RFC3339Nano) }

func parseState(stateJSON string) (*RequestState, error) {
	state := newRequestState("")
	if err := json.Unmarshal([]byte(stateJSON), state); err != nil {
		return nil, fmt.Errorf("stored state unreadable: %w", err)
	}
	return state, nil
}

func stateHash(stateJSON string) string {
	sum := sha256.Sum256([]byte(stateJSON))
	return hex.EncodeToString(sum[:])
}

func newRequestID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "req_" + hex.EncodeToString(b[:])
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func mustCanonical(raw []byte) []byte {
	canon, err := CanonicalJSON(raw)
	if err != nil {
		return raw
	}
	return canon
}
