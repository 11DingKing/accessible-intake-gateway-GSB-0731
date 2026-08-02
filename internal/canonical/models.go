// Package canonical defines the normalized intake envelope shared by every
// channel adapter, plus the per-item result and canonical request snapshot.
package canonical

import "time"

// Channel identifiers. These must match materials/channel-contracts.json.
const (
	ChannelPhysical = "PHYSICAL"
	ChannelHotline  = "HOTLINE"
	ChannelWeb      = "WEB"
)

// Attempt statuses stored for every submission.
const (
	AttemptReceived          = "RECEIVED"
	AttemptOK                = "OK"
	AttemptConflict          = "CONFLICT"
	AttemptValidationError   = "VALIDATION_ERROR"
	AttemptAdapterTransient  = "ADAPTER_TRANSIENT"
)

// Event processing statuses.
const (
	EventAccepted  = "ACCEPTED"
	EventRejected  = "REJECTED"
)

// ItemResult codes returned inside a submission response.
const (
	ItemOK                = "OK"
	ItemUnknownCode       = "UNKNOWN_CODE"
	ItemInvalidValue      = "INVALID_VALUE"
	ItemMissingField      = "MISSING_FIELD"
	ItemUnknownScope      = "UNKNOWN_SCOPE"
	ItemUnknownChannel    = "UNKNOWN_CHANNEL"
	ItemRevocationTarget  = "REVOCATION_TARGET_MISSING"
)

// Person is the normalized subject carried by visitor/caller/applicant.
type Person struct {
	FullName       string `json:"fullName,omitempty"`
	Phone          string `json:"phone,omitempty"`
	Email          string `json:"email,omitempty"`
	ReferenceNumber string `json:"referenceNumber,omitempty"`
}

// SourceRef records the native correlation handle for one event.
type SourceRef struct {
	Channel  string `json:"channel"`
	SourceID string `json:"sourceId"`
	Field    string `json:"sourceIdField"`
}

// ItemResult is the per-record success/failure outcome for one field or item.
type ItemResult struct {
	Item    string `json:"item"`
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// EventEnvelope is the inbound payload accepted from any channel adapter.
//
// It accepts both the native field names declared in the fixture
// (deskReceiptNo / callRef / submissionId, visitor / caller / applicant,
// callbackWindowMinutes) and the normalized aliases. After normalization the
// Channel and SourceID fields are authoritative.
type EventEnvelope struct {
	EventID             string   `json:"eventId"`
	Channel             string   `json:"channel"`
	CanonicalRequestID  string   `json:"canonicalRequestId,omitempty"`

	// Native source-id fields per channel.
	DeskReceiptNo string `json:"deskReceiptNo,omitempty"`
	CallRef       string `json:"callRef,omitempty"`
	SubmissionID  string `json:"submissionId,omitempty"`

	// Native person fields per channel.
	Visitor   *Person `json:"visitor,omitempty"`
	Caller    *Person `json:"caller,omitempty"`
	Applicant *Person `json:"applicant,omitempty"`

	// Normalized alias accepted on every channel.
	Person *Person `json:"person,omitempty"`

	Accommodations       []string `json:"accommodations,omitempty"`
	Consent              []string `json:"consent,omitempty"`
	CallbackWindowMinutes *int    `json:"callbackWindowMinutes,omitempty"`

	// Revocation: when set, this event revokes the listed scopes that were
	// granted by the referenced event.
	Revokes string   `json:"revokes,omitempty"`
	Scopes  []string `json:"scopes,omitempty"`

	// SimulateAdapterTransient is a test hook: when true the service records
	// an ADAPTER_TRANSIENT attempt and returns 503 without committing the
	// event. The next retry (with the flag cleared) is idempotent.
	SimulateAdapterTransient bool `json:"simulateAdapterTransient,omitempty"`
}

// NormalizedEvent is the canonical view derived from an envelope.
type NormalizedEvent struct {
	EventID            string
	Channel            string
	CanonicalRequestID string
	SourceID           string
	Person             Person
	Accommodations     []string
	Consent            []string
	CallbackWindow     *int
	Revokes            string
	RevocationScopes   []string
}

// Attempt is one submission attempt recorded for audit/replay.
type Attempt struct {
	ID          int64      `json:"id"`
	EventID     string     `json:"eventId"`
	AttemptNo   int        `json:"attemptNo"`
	RequestHash string     `json:"requestHash"`
	Status      string     `json:"status"`
	Errors      []ItemResult `json:"errors,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
}

// CanonicalSnapshot is the current merged state of a request chain.
type CanonicalSnapshot struct {
	ID                    string       `json:"id"`
	CanonicalVersion      string       `json:"canonicalVersion"`
	SubjectKey            string       `json:"subjectKey"`
	Person                Person       `json:"person"`
	Accommodations        []string     `json:"accommodations"`
	Consent               []string     `json:"consent"`
	CallbackWindowMinutes *int         `json:"callbackWindowMinutes,omitempty"`
	Sources               []SourceRef  `json:"sources"`
	Sequence              int          `json:"sequence"`
	CreatedAt             time.Time    `json:"createdAt"`
	UpdatedAt             time.Time    `json:"updatedAt"`
	ContactMasked         bool         `json:"contactMasked"`
}

// SubmissionResult is returned for every event POST (including idempotent replay).
type SubmissionResult struct {
	EventID            string       `json:"eventId"`
	Status             string       `json:"status"`
	CanonicalRequestID string       `json:"canonicalRequestId"`
	IdempotentReplay   bool         `json:"idempotentReplay,omitempty"`
	Conflict           *Conflict    `json:"conflict,omitempty"`
	ItemResults        []ItemResult `json:"itemResults"`
	Canonical          CanonicalSnapshot `json:"canonical"`
}

// Conflict describes a same-eventId/different-payload collision.
type Conflict struct {
	EventID        string `json:"eventId"`
	ExistingHash   string `json:"existingPayloadHash"`
	IncomingHash   string `json:"incomingPayloadHash"`
	Message        string `json:"message"`
}

// AuditSummary is the minimal replayable audit record for a canonical chain.
type AuditSummary struct {
	CanonicalRequestID string          `json:"canonicalRequestId"`
	SubjectKey         string          `json:"subjectKey"`
	EventCount         int             `json:"eventCount"`
	AttemptCount       int             `json:"attemptCount"`
	LatestSequence     int             `json:"latestSequence"`
	CurrentConsent     []string        `json:"currentConsent"`
	Accommodations     []string        `json:"accommodations"`
	Sources            []SourceRef     `json:"sources"`
	Events             []EventAudit    `json:"events"`
}

// EventAudit is one committed event in the chain, in replay order.
type EventAudit struct {
	Sequence    int       `json:"sequence"`
	EventID     string    `json:"eventId"`
	Channel     string    `json:"channel"`
	SourceID    string    `json:"sourceId"`
	PayloadHash string    `json:"payloadHash"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}
