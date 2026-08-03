// Package intake implements the unified intake service: it normalizes
// channel-specific envelopes, validates each item independently, enforces
// idempotency and conflict detection, merges events into a single canonical
// request chain using deterministic candidate matching with evidence, and
// records every attempt for audit and stable replay.
package intake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/accessible-intake/gateway/internal/canonical"
	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/store"
)

// Service is the unified intake service.
type Service struct {
	store     *store.Store
	contracts *contracts.Contracts
}

// New constructs a Service.
func New(s *store.Store, c *contracts.Contracts) *Service {
	return &Service{store: s, contracts: c}
}

// SubmitError carries an HTTP status and a machine-readable code back to the
// HTTP layer. Per-item validation errors are returned inside a successful
// SubmissionResult, not as a SubmitError.
type SubmitError struct {
	Status  int
	Code    string
	Message string
	Result  *canonical.SubmissionResult
}

func (e *SubmitError) Error() string { return e.Message }

// Submit accepts a channel event envelope and returns the normalized result.
//
// Transaction semantics (per item):
//   - Top-level fatal errors (missing eventId, unknown channel) reject the
//     whole submission; an attempt is recorded and no event is committed.
//   - Per-item errors (unknown accommodation code, unknown consent scope,
//     negative/cross-day callback window) do NOT abort the event. Valid items
//     are committed, invalid items appear in ItemResults, and the event is
//     ACCEPTED.
//   - Same eventId + same payload hash returns the original outcome (an
//     additional attempt is recorded).
//   - Same eventId + different payload hash returns 409 Conflict and commits
//     nothing beyond the attempt.
//   - A simulated adapter transient failure records an ADAPTER_TRANSIENT
//     attempt and returns 503 without committing the event.
//
// Candidate matching:
//   - For non-revocation events the service finds ALL existing canonical
//     chains sharing any identifier (reference number, normalized phone,
//     email), scores each deterministically, and auto-links to the highest
//     scoring candidate when the score is strong enough (>=80). Name-only
//     matches never auto-link. If no candidate qualifies, a new chain is
//     created. Every evaluated candidate with its reasons/conflicts is
//     persisted to match_candidates for replay.
//   - Two channels concurrently reporting the same applicant serialize on
//     BEGIN IMMEDIATE; the second transaction finds the chain created by the
//     first and appends to it, so exactly one canonical event chain exists.
func (s *Service) Submit(ctx context.Context, env canonical.EventEnvelope) (*canonical.SubmissionResult, error) {
	hash := canonical.StablePayloadHash(env)
	norm := canonical.Normalize(env)
	results, fatal := canonical.Validate(norm, s.contracts)

	if fatal {
		if err := s.recordAttemptOnly(ctx, norm.EventID, hash, canonical.AttemptValidationError, results); err != nil {
			return nil, err
		}
		return nil, &SubmitError{
			Status: 400, Code: "VALIDATION_ERROR",
			Message: "event rejected: " + joinMessages(results),
			Result: &canonical.SubmissionResult{
				EventID:     norm.EventID,
				Status:      canonical.EventRejected,
				ItemResults: results,
			},
		}
	}

	if env.SimulateAdapterTransient {
		if err := s.recordAttemptOnly(ctx, norm.EventID, hash, canonical.AttemptAdapterTransient, results); err != nil {
			return nil, err
		}
		return nil, &SubmitError{
			Status: 503, Code: "ADAPTER_TRANSIENT",
			Message: "channel adapter reported a transient failure; retry is safe and idempotent",
		}
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	attemptNo, err := tx.NextAttemptNo(ctx, norm.EventID)
	if err != nil {
		return nil, err
	}

	existing, err := tx.FindEvent(ctx, norm.EventID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if existing != nil {
		if existing.PayloadHash != hash {
			if err := tx.RecordAttempt(ctx, norm.EventID, attemptNo, hash, canonical.AttemptConflict, results); err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			snap, _ := s.store.BuildSnapshot(ctx, existing.CanonicalID, store.SnapshotOptions{RedactContacts: true})
			candidates, _ := s.store.FindMatchCandidates(ctx, norm.EventID)
			cb, _ := s.store.FindCallback(ctx, norm.EventID)
			return nil, &SubmitError{
				Status: 409, Code: "EVENT_CONFLICT",
				Message: "eventId already exists with a different payload",
				Result: &canonical.SubmissionResult{
					EventID:            norm.EventID,
					Status:             canonical.AttemptConflict,
					CanonicalRequestID: existing.CanonicalID,
					Conflict: &canonical.Conflict{
						EventID:      norm.EventID,
						ExistingHash: existing.PayloadHash,
						IncomingHash: hash,
						Message:      "same eventId with different payload is a conflict",
					},
					ItemResults:     results,
					Canonical:       snap,
					MatchCandidates: candidates,
					Callback:        s.callbackToAPI(ctx, cb),
				},
			}
		}
		_ = tx.RecordAttempt(ctx, norm.EventID, attemptNo, hash, canonical.AttemptOK, results)
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		itemResults, _ := s.store.EventItemResults(ctx, norm.EventID)
		snap, _ := s.store.BuildSnapshot(ctx, existing.CanonicalID, store.SnapshotOptions{RedactContacts: true})
		candidates, _ := s.store.FindMatchCandidates(ctx, norm.EventID)
		cb, _ := s.store.FindCallback(ctx, norm.EventID)
		return &canonical.SubmissionResult{
			EventID:            norm.EventID,
			Status:             existing.Status,
			CanonicalRequestID: existing.CanonicalID,
			IdempotentReplay:   true,
			ItemResults:        itemResults,
			Canonical:          snap,
			MatchCandidates:    candidates,
			Callback:           s.callbackToAPI(ctx, cb),
		}, nil
	}

	var canonicalID string
	var scored []canonical.ScoredCandidate
	var matchStatus string

	if norm.IsRevocation() {
		canonicalID, err = tx.CanonicalIDForEvent(ctx, norm.Revokes)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				results = append(results, canonical.ItemResult{
					Item:    "revokes",
					Code:    canonical.ItemRevocationTarget,
					Message: "revocation target event not found: " + norm.Revokes,
				})
				_ = tx.RecordAttempt(ctx, norm.EventID, attemptNo, hash, canonical.AttemptValidationError, results)
				if err := tx.Commit(); err != nil {
					return nil, err
				}
				return nil, &SubmitError{
					Status: 400, Code: "REVOCATION_TARGET_MISSING",
					Message: "revocation target event not found",
					Result: &canonical.SubmissionResult{
						EventID:     norm.EventID,
						Status:      canonical.EventRejected,
						ItemResults: results,
					},
				}
			}
			return nil, err
		}
		matchStatus = canonical.MatchLinked
	} else {
		identity := norm.IdentityOf()
		subjectKey := norm.SubjectKey()
		if subjectKey == "" {
			results = append(results, canonical.ItemResult{
				Item: "person", Code: canonical.ItemMissingField,
				Message: "at least one person identifier (referenceNumber, phone, email, or fullName) is required",
			})
			_ = tx.RecordAttempt(ctx, norm.EventID, attemptNo, hash, canonical.AttemptValidationError, results)
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return nil, &SubmitError{
				Status: 400, Code: "SUBJECT_KEY_MISSING",
				Message: "cannot derive canonical request without a person identifier",
				Result: &canonical.SubmissionResult{
					EventID:     norm.EventID,
					Status:      canonical.EventRejected,
					ItemResults: results,
				},
			}
		}

		candidates, err := tx.FindCandidatesByIdentity(ctx, identity.ReferenceNumber, identity.PhoneDigits, identity.Email, identity.FullNameNorm)
		if err != nil {
			return nil, err
		}
		for _, cr := range candidates {
			existingFrag := canonical.IdentityFragment{
				ReferenceNumber: cr.PersonRef,
				PhoneDigits:     cr.PhoneDigits,
				Email:           cr.Email,
				FullNameNorm:    normalizeName(cr.FullName),
			}
			sc := canonical.ScoreCandidate(identity, existingFrag)
			sc.CanonicalID = cr.ID
			scored = append(scored, sc)
		}
		canonical.RankCandidates(scored)

		// Auto-link to the highest-scoring candidate when it has a strong
		// identifier match (score >= 80), OR when there is exactly one
		// candidate with a name match (score >= 10) — this allows events
		// after a PII-clearing revocation to still find their chain by
		// name without risking ambiguous cross-person linkage.
		selected := (*canonical.ScoredCandidate)(nil)
		for i := range scored {
			if scored[i].CanAutoLink() {
				selected = &scored[i]
				break
			}
		}
		if selected == nil && len(scored) == 1 && scored[0].Score >= 10 {
			selected = &scored[0]
		}

		if selected != nil {
			canonicalID = selected.CanonicalID
			if selected.HasConflict() {
				matchStatus = canonical.MatchConflict
			} else {
				matchStatus = canonical.MatchLinked
			}
		} else {
			// No strong candidate from identity matching. As a fallback,
			// check whether a chain with the same subject_key already
			// exists. This handles the case where PII columns were cleared
			// by a revocation but the subject linkage remains (e.g. a late
			// hotline retry carrying a revoked phone number must not create
			// a second chain or violate the subject_key UNIQUE constraint).
			existing, err := tx.FindCanonicalBySubject(ctx, subjectKey)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return nil, err
			}
			if existing != nil {
				canonicalID = existing.ID
				matchStatus = canonical.MatchLinked
			} else {
				canonicalID = newCanonicalID()
				if err := tx.CreateCanonical(ctx, canonicalID, subjectKey, s.contracts.Version()); err != nil {
					return nil, err
				}
				matchStatus = canonical.MatchPending
			}
		}
	}

	seq, err := tx.NextSequence(ctx, canonicalID)
	if err != nil {
		return nil, err
	}

	attemptID, err := tx.RecordAttemptWithID(ctx, norm.EventID, attemptNo, hash, canonical.AttemptOK, results)
	if err != nil {
		return nil, err
	}

	selectedID := canonicalID
	matchCandidates := canonical.ToMatchCandidates(scored, selectedID)
	for i := range matchCandidates {
		if err := tx.InsertMatchCandidate(ctx, norm.EventID, matchCandidates[i]); err != nil {
			return nil, err
		}
	}

	contactGrantedBefore, err := tx.HasConsentScope(ctx, canonicalID, "CONTACT_CALLBACK")
	if err != nil {
		return nil, err
	}

	eventRow := store.EventRow{
		EventID:         norm.EventID,
		CanonicalID:     canonicalID,
		Channel:         norm.Channel,
		SourceID:        norm.SourceID,
		SourceIDField:   s.contracts.Channels[norm.Channel].SourceIDField,
		PayloadHash:     hash,
		Sequence:        seq,
		Status:          canonical.EventAccepted,
		ItemResultsJSON: mustJSON(results),
	}

	var windowStatus string
	var effectiveWindow *int
	if norm.Channel == canonical.ChannelHotline {
		windowStatus, effectiveWindow, _ = canonical.ClassifyCallbackWindow(norm.CallbackWindow)
	}

	if norm.IsRevocation() {
		eventRow.IsRevocation = true
		eventRow.Revokes = norm.Revokes
	} else {
		eventRow.PersonName = norm.Person.FullName
		eventRow.PersonPhone = norm.Person.Phone
		eventRow.PersonEmail = norm.Person.Email
		eventRow.PersonRef = norm.Person.ReferenceNumber
		eventRow.CallbackWindow = effectiveWindow
	}

	eventRow.PayloadJSON = mustJSON(env)

	if err := tx.InsertEvent(ctx, eventRow); err != nil {
		return nil, err
	}

	if norm.IsRevocation() {
		for _, scope := range norm.RevocationScopes {
			if s.contracts.IsConsentScope(scope) {
				if err := tx.RevokeConsent(ctx, canonicalID, norm.EventID, seq, scope); err != nil {
					return nil, err
				}
				for _, field := range canonical.FieldsForScope(scope) {
					if canonical.IsAccommodationField(field) {
						if err := tx.DeleteAccommodations(ctx, canonicalID); err != nil {
							return nil, err
						}
					} else {
						if err := tx.ClearCanonicalField(ctx, canonicalID, field); err != nil {
							return nil, err
						}
						if err := tx.RedactEventField(ctx, canonicalID, field); err != nil {
							return nil, err
						}
					}
					if err := tx.RevokeField(ctx, canonicalID, field, norm.EventID); err != nil {
						return nil, err
					}
				}
			}
		}
	} else {
		revoked, err := tx.RevokedFields(ctx, canonicalID)
		if err != nil {
			return nil, err
		}
		for _, code := range norm.Accommodations {
			if s.contracts.IsAccommodation(code) {
				if err := tx.AddAccommodationGuarded(ctx, canonicalID, norm.EventID, seq, code, revoked); err != nil {
					return nil, err
				}
			}
		}
		for _, scope := range norm.Consent {
			if s.contracts.IsConsentScope(scope) {
				if err := tx.GrantConsent(ctx, canonicalID, norm.EventID, seq, scope); err != nil {
					return nil, err
				}
			}
		}
		if err := tx.UpdateCanonicalPersonGuarded(ctx, canonicalID,
			norm.Person.FullName, norm.Person.Phone, norm.Person.Email,
			norm.Person.ReferenceNumber, effectiveWindow, revoked); err != nil {
			return nil, err
		}
	}

	if norm.Channel == canonical.ChannelHotline {
		contactGrantedAfter, err := tx.HasConsentScope(ctx, canonicalID, "CONTACT_CALLBACK")
		if err != nil {
			return nil, err
		}
		contactExposed := contactGrantedAfter
		confidence := canonical.ConfidenceNone
		if selected := findSelected(scored, selectedID); selected != nil {
			confidence = selected.Confidence
		}
		cb := canonical.CallbackRecord{
			EventID:            norm.EventID,
			CanonicalRequestID: canonicalID,
			Channel:            norm.Channel,
			SourceID:           norm.SourceID,
			SourceIDField:      s.contracts.Channels[norm.Channel].SourceIDField,
			CallbackWindow:     effectiveWindow,
			WindowUnit:         canonical.CallbackWindowUnit,
			WindowStatus:       windowStatus,
			MatchStatus:        matchStatus,
			Confidence:         confidence,
			Candidates:         matchCandidates,
			ContactExposed:     contactExposed,
			ConsentPending:     !contactGrantedBefore && !contactGrantedAfter,
			CreatedAt:          time.Now().UTC(),
		}
		if matchStatus == canonical.MatchLinked || matchStatus == canonical.MatchConflict {
			now := time.Now().UTC()
			cb.LinkedAt = &now
		}
		if err := tx.InsertCallback(ctx, cb); err != nil {
			return nil, err
		}
	}

	if err := tx.InsertValidationErrors(ctx, norm.EventID, attemptID, results); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	snap, err := s.store.BuildSnapshot(ctx, canonicalID, store.SnapshotOptions{RedactContacts: true})
	if err != nil {
		return nil, err
	}

	result := &canonical.SubmissionResult{
		EventID:            norm.EventID,
		Status:             canonical.EventAccepted,
		CanonicalRequestID: canonicalID,
		ItemResults:        results,
		Canonical:          snap,
		MatchCandidates:    matchCandidates,
	}
	if norm.Channel == canonical.ChannelHotline {
		cb, _ := s.store.FindCallback(ctx, norm.EventID)
		result.Callback = s.callbackToAPI(ctx, cb)
	}
	return result, nil
}

func findSelected(scored []canonical.ScoredCandidate, id string) *canonical.ScoredCandidate {
	for i := range scored {
		if scored[i].CanonicalID == id && scored[i].CanAutoLink() {
			return &scored[i]
		}
	}
	return nil
}

func (s *Service) callbackToAPI(ctx context.Context, row *store.CallbackRow) *canonical.CallbackRecord {
	if row == nil {
		return nil
	}
	contactGranted, _ := s.store.HasConsentScope(ctx, row.CanonicalRequestID, "CONTACT_CALLBACK")
	created, _ := time.Parse(time.RFC3339Nano, row.CreatedAt)
	cb := &canonical.CallbackRecord{
		EventID:            row.EventID,
		CanonicalRequestID: row.CanonicalRequestID,
		Channel:            row.Channel,
		SourceID:           row.SourceID,
		SourceIDField:      row.SourceIDField,
		CallbackWindow:     row.CallbackWindow,
		WindowUnit:         row.WindowUnit,
		WindowStatus:       row.WindowStatus,
		MatchStatus:        row.MatchStatus,
		Confidence:         row.Confidence,
		ContactExposed:     contactGranted,
		ConsentPending:     row.ConsentPending || !contactGranted,
		CreatedAt:          created,
	}
	if row.LinkedAt.Valid {
		t, _ := time.Parse(time.RFC3339Nano, row.LinkedAt.String)
		cb.LinkedAt = &t
	}
	cands, _ := s.store.FindMatchCandidates(ctx, row.EventID)
	cb.Candidates = cands
	return cb
}

func (s *Service) recordAttemptOnly(ctx context.Context, eventID, hash, status string, results []canonical.ItemResult) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	no, err := tx.NextAttemptNo(ctx, eventID)
	if err != nil {
		return err
	}
	if _, err := tx.RecordAttemptWithID(ctx, eventID, no, hash, status, results); err != nil {
		return err
	}
	return tx.Commit()
}

// Canonical returns the current masked canonical snapshot for a chain.
func (s *Service) Canonical(ctx context.Context, id string) (canonical.CanonicalSnapshot, error) {
	return s.store.BuildSnapshot(ctx, id, store.SnapshotOptions{RedactContacts: true})
}

// Audit returns the minimal audit summary for a chain.
func (s *Service) Audit(ctx context.Context, id string) (canonical.AuditSummary, error) {
	return s.store.AuditSummary(ctx, id)
}

// Replay reconstructs the canonical snapshot from the event log.
func (s *Service) Replay(ctx context.Context, id string) (canonical.CanonicalSnapshot, []canonical.EventAudit, error) {
	snap, err := s.store.BuildSnapshot(ctx, id, store.SnapshotOptions{RedactContacts: true})
	if err != nil {
		return canonical.CanonicalSnapshot{}, nil, err
	}
	audit, err := s.store.AuditSummary(ctx, id)
	if err != nil {
		return canonical.CanonicalSnapshot{}, nil, err
	}
	return snap, audit.Events, nil
}

// Event returns a committed event and its item results.
func (s *Service) Event(ctx context.Context, eventID string) (*store.EventRow, []canonical.ItemResult, error) {
	row, err := s.store.FindEvent(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	results, err := s.store.EventItemResults(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	return row, results, nil
}

// Attempts returns every attempt recorded for an event.
func (s *Service) Attempts(ctx context.Context, eventID string) ([]canonical.Attempt, error) {
	return s.store.Attempts(ctx, eventID)
}

// Callback returns the callback projection for an event.
func (s *Service) Callback(ctx context.Context, eventID string) (*canonical.CallbackRecord, error) {
	row, err := s.store.FindCallback(ctx, eventID)
	if err != nil {
		return nil, err
	}
	return s.callbackToAPI(ctx, row), nil
}

// CallbacksByCanonical returns all callbacks for a canonical chain.
func (s *Service) CallbacksByCanonical(ctx context.Context, canonicalID string) ([]*canonical.CallbackRecord, error) {
	rows, err := s.store.FindCallbacksByCanonical(ctx, canonicalID)
	if err != nil {
		return nil, err
	}
	out := make([]*canonical.CallbackRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, s.callbackToAPI(ctx, r))
	}
	return out, nil
}

// MatchCandidates returns the evaluated candidates for an event.
func (s *Service) MatchCandidates(ctx context.Context, eventID string) ([]canonical.MatchCandidate, error) {
	return s.store.FindMatchCandidates(ctx, eventID)
}

func normalizeName(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

func joinMessages(results []canonical.ItemResult) string {
	msg := ""
	for _, r := range results {
		if r.Code == canonical.ItemOK {
			continue
		}
		if msg != "" {
			msg += "; "
		}
		msg += r.Item + ": " + r.Message
	}
	if msg == "" {
		msg = "invalid event"
	}
	return msg
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func newCanonicalID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "CR-" + hex.EncodeToString(b[:])
}
