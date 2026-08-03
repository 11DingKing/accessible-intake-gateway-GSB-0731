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
	ItemCrossDayWindow    = "CROSS_DAY_WINDOW"
)

// CallbackWindowMinutes is the unit declared by the fixture (round 1).
// Window values are always interpreted in minutes.
const (
	CallbackWindowUnit        = "MINUTES"
	MaxCallbackWindowMinutes  = 1440 // 24 hours; >= this is a cross-day window
)

// Callback window validation outcomes.
const (
	WindowOK               = "OK"
	WindowZero             = "ZERO"
	WindowNegativeRejected = "NEGATIVE_REJECTED"
	WindowCrossDayRejected = "CROSS_DAY_REJECTED"
	WindowAbsent           = "ABSENT"
)

// Candidate match statuses.
const (
	MatchLinked   = "LINKED"
	MatchPending  = "PENDING"
	MatchConflict = "CONFLICT"
)

// Confidence levels for candidate normalization.
const (
	ConfidenceHigh   = "HIGH"
	ConfidenceMedium = "MEDIUM"
	ConfidenceLow    = "LOW"
	ConfidenceNone   = "NONE"
)

// Deterministic match reason codes.
const (
	ReasonReferenceExact   = "REFERENCE_EXACT_MATCH"
	ReasonPhoneExact       = "PHONE_EXACT_MATCH"
	ReasonEmailExact       = "EMAIL_EXACT_MATCH"
	ReasonNameExact        = "NAME_EXACT_MATCH"
	ReasonIdentityConflict = "IDENTITY_FRAGMENT_CONFLICT"
)

// IdentityFragment holds the normalized identifiers used for candidate
// matching. All values are pre-normalized (digits-only phone, lower-case
// email, collapsed name) by the caller.
type IdentityFragment struct {
	ReferenceNumber string
	PhoneDigits     string
	Email           string
	FullNameNorm    string
}

// MatchEvidence is one deterministic reason or conflict recorded for a
// candidate. It never carries raw PII; Evidence describes the matched field
// and whether the comparison was exact, not the values themselves.
type MatchEvidence struct {
	Field      string `json:"field"`
	Reason     string `json:"reason"`
	Confidence string `json:"confidence"`
	Detail     string `json:"detail,omitempty"`
}

// MatchCandidate is one canonical request considered during normalization.
type MatchCandidate struct {
	CanonicalRequestID string          `json:"canonicalRequestId"`
	Rank               int             `json:"rank"`
	Selected           bool            `json:"selected"`
	Confidence         string          `json:"confidence"`
	Score              int             `json:"score"`
	Reasons            []MatchEvidence `json:"reasons"`
	Conflicts          []MatchEvidence `json:"conflicts"`
}

// CallbackRecord is the persisted projection of a hotline callback event.
type CallbackRecord struct {
	EventID            string          `json:"eventId"`
	CanonicalRequestID string          `json:"canonicalRequestId"`
	Channel            string          `json:"channel"`
	SourceID           string          `json:"sourceId"`
	SourceIDField      string          `json:"sourceIdField"`
	CallbackWindow     *int            `json:"callbackWindowMinutes"`
	WindowUnit         string          `json:"windowUnit"`
	WindowStatus       string          `json:"windowStatus"`
	MatchStatus        string          `json:"matchStatus"`
	Confidence         string          `json:"confidence"`
	Candidates         []MatchCandidate `json:"candidates"`
	ContactExposed     bool            `json:"contactExposed"`
	ConsentPending     bool            `json:"consentPending"`
	CreatedAt          time.Time       `json:"createdAt"`
	LinkedAt           *time.Time      `json:"linkedAt,omitempty"`
}

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
	RevokedFields         []string     `json:"revokedFields,omitempty"`
}

// SubmissionResult is returned for every event POST (including idempotent replay).
type SubmissionResult struct {
	EventID            string            `json:"eventId"`
	Status             string            `json:"status"`
	CanonicalRequestID string            `json:"canonicalRequestId"`
	IdempotentReplay   bool              `json:"idempotentReplay,omitempty"`
	Conflict           *Conflict         `json:"conflict,omitempty"`
	ItemResults        []ItemResult      `json:"itemResults"`
	Canonical          CanonicalSnapshot `json:"canonical"`
	Callback           *CallbackRecord   `json:"callback,omitempty"`
	MatchCandidates    []MatchCandidate  `json:"matchCandidates,omitempty"`
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
	RevokedFields      []string        `json:"revokedFields,omitempty"`
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
