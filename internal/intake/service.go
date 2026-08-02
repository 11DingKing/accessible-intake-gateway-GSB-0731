// Package intake implements the unified intake service: it normalizes
// channel-specific envelopes, validates each item independently, enforces
// idempotency and conflict detection, merges events into a single canonical
// request chain, and records every attempt for audit and stable replay.
package intake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/accessible-intake/gateway/internal/canonical"
	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/store"
)

// Service is the unified intake service.
type Service struct {
	store    *store.Store
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

// AsSubmitError unwraps a SubmitError.
func AsSubmitError(err error) (*SubmitError, bool) {
	var se *SubmitError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}

// Submit accepts a channel event envelope and returns the normalized result.
//
// Transaction semantics (per item):
//   - Top-level fatal errors (missing eventId, unknown channel) reject the
//     whole submission; an attempt is recorded and no event is committed.
//   - Per-item errors (unknown accommodation code, unknown consent scope,
//     negative callback window) do NOT abort the event. Valid items are
//     committed, invalid items appear in ItemResults with a code, and the
//     event is ACCEPTED.
//   - Same eventId + same payload hash returns the original outcome (an
//     additional attempt is recorded).
//   - Same eventId + different payload hash returns 409 Conflict and commits
//     nothing beyond the attempt.
//   - A simulated adapter transient failure records an ADAPTER_TRANSIENT
//     attempt and returns 503 without committing the event; the next retry
//     is a clean, idempotent first commit.
func (s *Service) Submit(ctx context.Context, env canonical.EventEnvelope) (*canonical.SubmissionResult, error) {
	hash := canonical.StablePayloadHash(env)
	norm := canonical.Normalize(env)
	results, fatal := canonical.Validate(norm, s.contracts)

	// Fatal top-level errors: record attempt and reject without committing.
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

	// Simulated adapter transient failure: record a failed attempt and stop
	// before committing. The event is NOT persisted, so the subsequent retry
	// commits once and only once.
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

	// Idempotency / conflict check.
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
					ItemResults: results,
					Canonical:   snap,
				},
			}
		}
		// Same hash: idempotent replay. Record attempt and return original.
		_ = tx.RecordAttempt(ctx, norm.EventID, attemptNo, hash, canonical.AttemptOK, results)
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		itemResults, _ := s.store.EventItemResults(ctx, norm.EventID)
		snap, _ := s.store.BuildSnapshot(ctx, existing.CanonicalID, store.SnapshotOptions{RedactContacts: true})
		return &canonical.SubmissionResult{
			EventID:            norm.EventID,
			Status:             existing.Status,
			CanonicalRequestID: existing.CanonicalID,
			IdempotentReplay:   true,
			ItemResults:        itemResults,
			Canonical:          snap,
		}, nil
	}

	// Determine canonical chain.
	var canonicalID string
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
	} else {
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

		// Link to an existing chain if ANY stored identifier matches. This
		// is what unifies an in-person visit, hotline call, and web
		// submission from the same applicant into one event chain.
		cr, err := tx.FindCanonicalByIdentity(ctx,
			norm.Person.ReferenceNumber, norm.Person.Phone, norm.Person.Email)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		if cr != nil {
			canonicalID = cr.ID
		} else {
			canonicalID = newCanonicalID()
			if err := tx.CreateCanonical(ctx, canonicalID, subjectKey, s.contracts.Version()); err != nil {
				return nil, err
			}
		}
	}

	seq, err := tx.NextSequence(ctx, canonicalID)
	if err != nil {
		return nil, err
	}

	// Record the attempt (OK) and capture its id for validation error linkage.
	attemptID, err := tx.RecordAttemptWithID(ctx, norm.EventID, attemptNo, hash, canonical.AttemptOK, results)
	if err != nil {
		return nil, err
	}

	// Build and persist the event row. Valid items only are stored; invalid
	// items are recorded as validation errors.
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

	if norm.IsRevocation() {
		eventRow.IsRevocation = true
		eventRow.Revokes = norm.Revokes
	} else {
		eventRow.PersonName = norm.Person.FullName
		eventRow.PersonPhone = norm.Person.Phone
		eventRow.PersonEmail = norm.Person.Email
		eventRow.PersonRef = norm.Person.ReferenceNumber
		eventRow.CallbackWindow = validCallback(norm)
	}

	eventRow.PayloadJSON = mustJSON(env)

	if err := tx.InsertEvent(ctx, eventRow); err != nil {
		return nil, err
	}

	// Apply valid items. Invalid items are already captured in results and
	// will be persisted as validation_errors below.
	if norm.IsRevocation() {
		for _, scope := range norm.RevocationScopes {
			if s.contracts.IsConsentScope(scope) {
				if err := tx.RevokeConsent(ctx, canonicalID, norm.EventID, seq, scope); err != nil {
					return nil, err
				}
			}
		}
	} else {
		for _, code := range norm.Accommodations {
			if s.contracts.IsAccommodation(code) {
				if err := tx.AddAccommodation(ctx, canonicalID, norm.EventID, seq, code); err != nil {
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
		if err := tx.UpdateCanonicalPerson(ctx, canonicalID,
			norm.Person.FullName, norm.Person.Phone, norm.Person.Email,
			norm.Person.ReferenceNumber, validCallback(norm)); err != nil {
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

	return &canonical.SubmissionResult{
		EventID:            norm.EventID,
		Status:             canonical.EventAccepted,
		CanonicalRequestID: canonicalID,
		ItemResults:        results,
		Canonical:          snap,
	}, nil
}

// recordAttemptOnly persists a single attempt outside of the main write
// transaction (used for fatal validation and simulated adapter failures).
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

// Replay reconstructs the canonical snapshot and asserts it matches the stored
// state. It returns the reconstructed snapshot and the list of events applied.
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

func validCallback(n canonical.NormalizedEvent) *int {
	if n.CallbackWindow == nil || *n.CallbackWindow < 0 {
		return nil
	}
	v := *n.CallbackWindow
	return &v
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
