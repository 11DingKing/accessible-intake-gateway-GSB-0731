// Package model holds the canonical request domain types shared across the
// normalization, storage, and service layers.
package model

import "encoding/json"

// Status is the per-record processing outcome for a single intake event.
type Status string

const (
	// StatusAccepted means the event was validated and applied to a canonical request.
	StatusAccepted Status = "ACCEPTED"
	// StatusRejected means the event failed validation; the outcome is terminal and replayable.
	StatusRejected Status = "REJECTED"
	// StatusConflict means the same event key arrived with a different payload.
	StatusConflict Status = "CONFLICT"
	// StatusFailed means a transient downstream adapter error occurred; the event is retriable.
	StatusFailed Status = "FAILED"
)

// EventKind distinguishes ordinary intake events from consent revocations.
type EventKind string

const (
	// KindStandard is an ordinary intake event carrying accommodations/consent.
	KindStandard EventKind = "STANDARD"
	// KindRevocation withdraws one or more consent scopes from a canonical request.
	KindRevocation EventKind = "REVOCATION"
)

// RecordError is a single, field-scoped validation failure.
type RecordError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// CanonicalEvent is a channel-neutral event produced by the normalizers. The
// per-channel source field names (deskReceiptNo/callRef/submissionId,
// visitor/caller/applicant, callbackWindowMinutes) are preserved as metadata
// rather than renamed.
type CanonicalEvent struct {
	EventID       string          `json:"eventId"`
	Channel       string          `json:"channel"`
	Kind          EventKind       `json:"kind"`
	CanonicalKey  string          `json:"canonicalKey"`
	SourceIDField string          `json:"sourceIdField"`
	SourceID      string          `json:"sourceId"`
	PersonField   string          `json:"personField"`
	Person        json.RawMessage `json:"person,omitempty"`

	Accommodations []string `json:"accommodations,omitempty"`
	ConsentGrants  []string `json:"consentGrants,omitempty"`
	ConsentRevokes []string `json:"consentRevokes,omitempty"`
	RevokesEventID string   `json:"revokesEventId,omitempty"`

	// CallbackWindowMinutes is the hotline minute-based callback field. It is a
	// pointer so that "absent" is distinguishable from an explicit zero.
	CallbackWindowMinutes *int `json:"callbackWindowMinutes,omitempty"`

	// Fragments are the normalized identity fragments carried by this event
	// (source correlation number, phone, email, name parts, etc.). They drive
	// deterministic matching to an existing canonical request. Sensitive
	// fragments store only a hash here; raw contact values live in RawContacts
	// and are never persisted or exposed before consent.
	Fragments []IdentityFragment `json:"fragments,omitempty"`

	// RawContacts holds the plaintext of sensitive fragments (phone/email),
	// keyed by fragment type. It is used only to populate the consent-gated
	// contact projection and is never written to the audit trail or evidence.
	RawContacts map[string]string `json:"-"`

	// PayloadHash is a canonical digest of the raw envelope used for idempotency.
	PayloadHash string `json:"payloadHash"`
}

// IdentityFragment is one normalized identity signal on an event.
type IdentityFragment struct {
	Type      string `json:"type"`
	Hash      string `json:"hash"`
	Unique    bool   `json:"unique"`
	Sensitive bool   `json:"sensitive"`
	// Value carries the plaintext only for non-sensitive fragments; sensitive
	// fragments leave it empty so raw contact data is never surfaced.
	Value string `json:"value,omitempty"`
}

// MatchDecision is the deterministic resolution outcome for an event: which
// canonical request it bound to, why, and with what confidence evidence.
type MatchDecision struct {
	CanonicalKey  string   `json:"canonicalKey"`
	Reason        string   `json:"reason"`
	Confidence    string   `json:"confidence"`
	Score         float64  `json:"score"`
	MatchedOn     []string `json:"matchedOn,omitempty"`
	ConflictingOn []string `json:"conflictingOn,omitempty"`
	NewRequest    bool     `json:"newRequest"`
}

// RecordResult is the outcome returned to the caller for one submitted event.
type RecordResult struct {
	EventID      string         `json:"eventId"`
	Channel      string         `json:"channel"`
	Status       Status         `json:"status"`
	CanonicalKey string         `json:"canonicalKey,omitempty"`
	Duplicate    bool           `json:"duplicate,omitempty"`
	Retriable    bool           `json:"retriable,omitempty"`
	AttemptNo    int            `json:"attemptNo"`
	Match        *MatchDecision `json:"match,omitempty"`
	Errors       []RecordError  `json:"errors,omitempty"`
}

// ChainLink is one accepted event within a canonical request's event chain.
type ChainLink struct {
	Seq           int64     `json:"seq"`
	EventID       string    `json:"eventId"`
	Channel       string    `json:"channel"`
	Kind          EventKind `json:"kind"`
	SourceIDField string    `json:"sourceIdField"`
	SourceID      string    `json:"sourceId"`
}

// ContactProjection carries contact/callback details. Raw contact methods and
// the callback window are exposed only while the CONTACT_CALLBACK consent scope
// is effective; before consent, only the non-identifying disposition and a
// masked availability flag are shown.
type ContactProjection struct {
	Exposed               bool     `json:"exposed"`
	CallbackWindowMinutes *int     `json:"callbackWindowMinutes,omitempty"`
	CallbackDisposition   string   `json:"callbackDisposition,omitempty"`
	// HasPendingContact reports that raw contact info is held but withheld
	// pending consent. It carries no identifying value.
	HasPendingContact bool     `json:"hasPendingContact,omitempty"`
	Methods           []string `json:"methods,omitempty"`
}

// CanonicalRequest is the normalized aggregate for a single applicant/request,
// converged from every channel event that shares its canonical key.
type CanonicalRequest struct {
	ID               int64             `json:"id"`
	CanonicalKey     string            `json:"canonicalKey"`
	Version          string            `json:"canonicalVersion"`
	Chain            []ChainLink       `json:"chain"`
	Accommodations   []string          `json:"accommodations"`
	EffectiveConsent []string          `json:"effectiveConsent"`
	RevokedConsent   []string          `json:"revokedConsent"`
	Contact          ContactProjection `json:"contact"`
	CreatedSeq       int64             `json:"createdSeq"`
	UpdatedSeq       int64             `json:"updatedSeq"`
}

// Attempt is one append-only processing attempt for an event.
type Attempt struct {
	AttemptNo    int           `json:"attemptNo"`
	EventID      string        `json:"eventId"`
	CanonicalKey string        `json:"canonicalKey,omitempty"`
	Status       Status        `json:"status"`
	Errors       []RecordError `json:"errors,omitempty"`
	PayloadHash  string        `json:"payloadHash"`
	At           string        `json:"at"`
}

// AuditEntry is one append-only audit record scoped to a canonical request.
type AuditEntry struct {
	Seq          int64  `json:"seq"`
	CanonicalKey string `json:"canonicalKey"`
	EventID      string `json:"eventId"`
	Action       string `json:"action"`
	Detail       string `json:"detail,omitempty"`
	At           string `json:"at"`
}

// AuditSummary is the minimal, replay-stable digest for a canonical request.
type AuditSummary struct {
	CanonicalKey  string           `json:"canonicalKey"`
	Request       CanonicalRequest `json:"request"`
	Attempts      []Attempt        `json:"attempts"`
	Entries       []AuditEntry     `json:"auditTrail"`
	MatchEvidence []MatchDecision  `json:"matchEvidence"`
	AcceptedCount int              `json:"acceptedCount"`
	Digest        string           `json:"digest"`
}
