package intake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/accessible-intake/gateway/internal/contracts"
)

const (
	StatusAccepted           = "accepted"
	StatusAcceptedWithErrors = "accepted_with_errors"
	StatusConflict           = "conflict"
	StatusAdapterFailure     = "adapter_transient_failure"
	StatusBadRequest         = "bad_request"

	ItemApplied  = "applied"
	ItemRejected = "rejected"

	DeliveryPending   = "pending"
	DeliveryDelivered = "delivered"
	DeliveryFailed    = "failed"
)

type Person struct {
	FirstName         string `json:"firstName,omitempty"`
	LastName          string `json:"lastName,omitempty"`
	DateOfBirth       string `json:"dateOfBirth,omitempty"`
	Email             string `json:"email,omitempty"`
	Phone             string `json:"phone,omitempty"`
	PreferredLanguage string `json:"preferredLanguage,omitempty"`
}

type Envelope struct {
	EventID               string
	Channel               string
	SourceID              string
	SourceIDField         string
	Person                Person
	Accommodations        []string
	Consent               []string
	CallbackWindowMinutes *int
	RevokesEventID        string
	RevocationScopes      []string
	Summary               string
	CanonicalRequestID    string
	ReceivedAt            time.Time
}

type ItemResult struct {
	Field   string `json:"field"`
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Value   any    `json:"value,omitempty"`
}

type AttemptRecord struct {
	AttemptNo int       `json:"attemptNo"`
	Outcome   string    `json:"outcome"`
	Error     string    `json:"error,omitempty"`
	At        time.Time `json:"at"`
}

type EventResult struct {
	EventID            string          `json:"eventId"`
	CanonicalRequestID string          `json:"canonicalRequestId"`
	Channel            string          `json:"channel"`
	SourceID           string          `json:"sourceId"`
	Status             string          `json:"status"`
	ItemResults        []ItemResult    `json:"itemResults"`
	PayloadHash        string          `json:"payloadHash"`
	Sequence           int64           `json:"sequence"`
	DeliveryStatus     string          `json:"deliveryStatus"`
	Attempts           []AttemptRecord `json:"attempts,omitempty"`
	CreatedAt          time.Time       `json:"createdAt"`
}

type CanonicalView struct {
	RequestID                 string            `json:"requestId"`
	Person                    Person            `json:"person"`
	ContactAvailable          bool              `json:"contactAvailable"`
	Accommodations            []string          `json:"accommodations"`
	ConsentScopes             []string          `json:"consentScopes"`
	SourceReferences          map[string]string `json:"sourceReferences"`
	CallbackWindowMinutes     *int              `json:"callbackWindowMinutes,omitempty"`
	Summary                   string            `json:"summary,omitempty"`
	SummaryTransferable       bool              `json:"summaryTransferable"`
	AccommodationTransferable bool              `json:"accommodationTransferable"`
	EventChain                []string          `json:"eventChain"`
	Sequence                  int64             `json:"sequence"`
	UpdatedAt                 time.Time         `json:"updatedAt"`
	ProjectionHash            string            `json:"projectionHash"`
}

type AppliedEvent struct {
	Sequence              int64
	EventID               string
	Channel               string
	SourceID              string
	Person                Person
	Accommodations        []string
	Consent               []string
	CallbackWindowMinutes *int
	RevokesEventID        string
	RevocationScopes      []string
	Summary               string
	ArrivalOrder          int64
	CreatedAt             time.Time
}

var (
	dobRe   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	phoneRe = regexp.MustCompile(`\D+`)
	spaceRe = regexp.MustCompile(`\s+`)
)

func normalizeName(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = spaceRe.ReplaceAllString(s, " ")
	return s
}

func normalizePhone(s string) string {
	return phoneRe.ReplaceAllString(s, "")
}

func PersonKey(p Person) string {
	fn := normalizeName(p.FirstName)
	ln := normalizeName(p.LastName)
	dob := strings.TrimSpace(p.DateOfBirth)
	if fn == "" || ln == "" || dob == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(fn + "|" + ln + "|" + dob))
	return hex.EncodeToString(sum[:])
}

func stableHash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// mergePerson fills empty fields of dst from src. The first value established
// for each field wins, so a later fragment that conflicts on identity never
// silently overwrites the canonical person; conflicts are recorded separately
// through the identity-matching evidence.
func mergePerson(dst *Person, src Person) {
	if dst.FirstName == "" {
		dst.FirstName = src.FirstName
	}
	if dst.LastName == "" {
		dst.LastName = src.LastName
	}
	if dst.DateOfBirth == "" {
		dst.DateOfBirth = src.DateOfBirth
	}
	if dst.Email == "" {
		dst.Email = src.Email
	}
	if dst.Phone == "" {
		dst.Phone = src.Phone
	}
	if dst.PreferredLanguage == "" {
		dst.PreferredLanguage = src.PreferredLanguage
	}
}

// DecodeEnvelope extracts a normalized envelope from raw channel JSON using
// the channel contract field names, preserving source field naming.
func DecodeEnvelope(raw []byte, c *contracts.Contracts) (*Envelope, []ItemResult, error) {
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, nil, fmt.Errorf("invalid JSON: %w", err)
	}

	var env Envelope
	env.ReceivedAt = time.Now().UTC()
	var hard []ItemResult

	if v, ok := generic["eventId"]; ok {
		_ = json.Unmarshal(v, &env.EventID)
		env.EventID = strings.TrimSpace(env.EventID)
	}
	if env.EventID == "" {
		hard = append(hard, ItemResult{Field: "eventId", Status: ItemRejected, Code: "MISSING_EVENT_ID", Message: "eventId is required"})
	}

	if v, ok := generic["channel"]; ok {
		_ = json.Unmarshal(v, &env.Channel)
		env.Channel = strings.TrimSpace(env.Channel)
	}
	if env.Channel == "" {
		hard = append(hard, ItemResult{Field: "channel", Status: ItemRejected, Code: "MISSING_CHANNEL", Message: "channel is required"})
	} else if !c.IsChannel(env.Channel) {
		hard = append(hard, ItemResult{Field: "channel", Status: ItemRejected, Code: "UNKNOWN_CHANNEL", Message: "unknown channel: " + env.Channel, Value: env.Channel})
	}

	if v, ok := generic["canonicalRequestId"]; ok {
		_ = json.Unmarshal(v, &env.CanonicalRequestID)
		env.CanonicalRequestID = strings.TrimSpace(env.CanonicalRequestID)
	}
	if v, ok := generic["summary"]; ok {
		_ = json.Unmarshal(v, &env.Summary)
	}

	if env.Channel != "" && c.IsChannel(env.Channel) {
		spec := c.Channels[env.Channel]
		env.SourceIDField = spec.SourceIDField
		if v, ok := generic[spec.SourceIDField]; ok {
			_ = json.Unmarshal(v, &env.SourceID)
			env.SourceID = strings.TrimSpace(env.SourceID)
		}
		if env.SourceID == "" {
			hard = append(hard, ItemResult{Field: spec.SourceIDField, Status: ItemRejected, Code: "MISSING_SOURCE_ID", Message: spec.SourceIDField + " is required for channel " + env.Channel})
		}
		if v, ok := generic[spec.PersonField]; ok {
			_ = json.Unmarshal(v, &env.Person)
		}
		if spec.CallbackWindowField != "" {
			if v, ok := generic[spec.CallbackWindowField]; ok {
				var n int
				if err := json.Unmarshal(v, &n); err != nil {
					hard = append(hard, ItemResult{Field: spec.CallbackWindowField, Status: ItemRejected, Code: "INVALID_CALLBACK_WINDOW", Message: "callback window must be an integer number of minutes"})
				} else {
					env.CallbackWindowMinutes = &n
				}
			}
		}
	}

	if v, ok := generic["accommodations"]; ok {
		_ = json.Unmarshal(v, &env.Accommodations)
	}
	if v, ok := generic["consent"]; ok {
		_ = json.Unmarshal(v, &env.Consent)
	}
	if v, ok := generic["revokes"]; ok {
		_ = json.Unmarshal(v, &env.RevokesEventID)
		env.RevokesEventID = strings.TrimSpace(env.RevokesEventID)
	}
	if v, ok := generic["revocationScopes"]; ok {
		_ = json.Unmarshal(v, &env.RevocationScopes)
	} else if env.RevokesEventID != "" {
		if v, ok := generic["scopes"]; ok {
			_ = json.Unmarshal(v, &env.RevocationScopes)
		}
	}

	if len(hard) > 0 {
		return &env, hard, fmt.Errorf("invalid envelope")
	}
	return &env, nil, nil
}

// Validate checks the envelope item by item and returns per-item results.
// Valid items are normalized in place; invalid items are reported and dropped
// from the applied contribution.
func Validate(env *Envelope, c *contracts.Contracts) []ItemResult {
	var results []ItemResult

	p := env.Person
	p.FirstName = strings.TrimSpace(p.FirstName)
	p.LastName = strings.TrimSpace(p.LastName)
	p.DateOfBirth = strings.TrimSpace(p.DateOfBirth)
	p.Email = strings.TrimSpace(strings.ToLower(p.Email))
	p.Phone = normalizePhone(p.Phone)
	p.PreferredLanguage = strings.TrimSpace(p.PreferredLanguage)

	if p.FirstName == "" || p.LastName == "" || p.DateOfBirth == "" {
		results = append(results, ItemResult{Field: "person", Status: ItemRejected, Code: "MISSING_PERSON_IDENTITY", Message: "firstName, lastName and dateOfBirth are required for cross-channel correlation"})
	} else if !dobRe.MatchString(p.DateOfBirth) {
		results = append(results, ItemResult{Field: "person.dateOfBirth", Status: ItemRejected, Code: "INVALID_DATE_OF_BIRTH", Message: "dateOfBirth must be YYYY-MM-DD", Value: p.DateOfBirth})
	} else {
		results = append(results, ItemResult{Field: "person", Status: ItemApplied})
	}
	env.Person = p

	validAcc := []string{}
	seenAcc := map[string]bool{}
	for _, code := range env.Accommodations {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		if !c.IsAccommodation(code) {
			results = append(results, ItemResult{Field: "accommodations", Status: ItemRejected, Code: "UNKNOWN_ACCOMMODATION_CODE", Message: "unknown accommodation code", Value: code})
			continue
		}
		if seenAcc[code] {
			continue
		}
		seenAcc[code] = true
		validAcc = append(validAcc, code)
		results = append(results, ItemResult{Field: "accommodations", Status: ItemApplied, Value: code})
	}
	env.Accommodations = validAcc

	validConsent := []string{}
	seenConsent := map[string]bool{}
	for _, scope := range env.Consent {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if !c.IsConsentScope(scope) {
			results = append(results, ItemResult{Field: "consent", Status: ItemRejected, Code: "UNKNOWN_CONSENT_SCOPE", Message: "unknown consent scope", Value: scope})
			continue
		}
		if seenConsent[scope] {
			continue
		}
		seenConsent[scope] = true
		validConsent = append(validConsent, scope)
		results = append(results, ItemResult{Field: "consent", Status: ItemApplied, Value: scope})
	}
	env.Consent = validConsent

	if env.CallbackWindowMinutes != nil {
		switch v := *env.CallbackWindowMinutes; {
		case v < 0:
			results = append(results, ItemResult{Field: "callbackWindowMinutes", Status: ItemRejected, Code: "INVALID_CALLBACK_WINDOW", Message: "callback window must not be negative", Value: v})
			env.CallbackWindowMinutes = nil
		case v == 0:
			results = append(results, ItemResult{Field: "callbackWindowMinutes", Status: ItemApplied, Code: "IMMEDIATE_CALLBACK", Message: "callback window of 0 means immediate callback", Value: v})
		case v > MaxCallbackWindowMinutes:
			results = append(results, ItemResult{Field: "callbackWindowMinutes", Status: ItemRejected, Code: "CROSS_DAY_CALLBACK_WINDOW", Message: "callback window exceeds 24 hours (1440 minutes) and is not same-day", Value: v})
			env.CallbackWindowMinutes = nil
		default:
			results = append(results, ItemResult{Field: "callbackWindowMinutes", Status: ItemApplied, Value: v})
		}
	}

	if env.RevokesEventID != "" {
		validRev := []string{}
		for _, scope := range env.RevocationScopes {
			scope = strings.TrimSpace(scope)
			if scope == "" {
				continue
			}
			if !c.IsConsentScope(scope) {
				results = append(results, ItemResult{Field: "revocationScopes", Status: ItemRejected, Code: "UNKNOWN_CONSENT_SCOPE", Message: "unknown revocation scope", Value: scope})
				continue
			}
			validRev = append(validRev, scope)
			results = append(results, ItemResult{Field: "revocationScopes", Status: ItemApplied, Value: scope})
		}
		env.RevocationScopes = validRev
	}

	return results
}

// PayloadHash produces a deterministic hash of the normalized envelope so that
// identical event keys with identical payloads are recognized, while a changed
// payload under the same key is a conflict.
func PayloadHash(env *Envelope) string {
	type canonical struct {
		EventID               string   `json:"eventId"`
		Channel               string   `json:"channel"`
		SourceID              string   `json:"sourceId"`
		Person                Person   `json:"person"`
		Accommodations        []string `json:"accommodations"`
		Consent               []string `json:"consent"`
		CallbackWindowMinutes *int     `json:"callbackWindowMinutes,omitempty"`
		RevokesEventID        string   `json:"revokes,omitempty"`
		RevocationScopes      []string `json:"revocationScopes,omitempty"`
		Summary               string   `json:"summary,omitempty"`
		CanonicalRequestID    string   `json:"canonicalRequestId,omitempty"`
	}
	return stableHash(canonical{
		EventID: env.EventID, Channel: env.Channel, SourceID: env.SourceID,
		Person: env.Person, Accommodations: env.Accommodations, Consent: env.Consent,
		CallbackWindowMinutes: env.CallbackWindowMinutes, RevokesEventID: env.RevokesEventID,
		RevocationScopes: env.RevocationScopes, Summary: env.Summary,
		CanonicalRequestID: env.CanonicalRequestID,
	})
}

func projectionOrder(events []*AppliedEvent) []*AppliedEvent {
	indexByID := make(map[string]int, len(events))
	for i, e := range events {
		indexByID[e.EventID] = i
	}
	// Build a topological order: a revocation referencing another event must be
	// projected immediately after that referenced event. If multiple revocations
	// target the same event they keep their relative arrival order. Out-of-order
	// arrival (revocation persisted before the grant it revokes) is handled by
	// reinserting the revocation right after the referenced grant.
	placed := make(map[string]bool, len(events))
	order := make([]string, 0, len(events))
	position := make(map[string]int, len(events))

	var visit func(id string, stack map[string]bool)
	visit = func(id string, stack map[string]bool) {
		if placed[id] {
			return
		}
		if stack[id] {
			return
		}
		stack[id] = true
		idx := indexByID[id]
		e := events[idx]
		if e.RevokesEventID != "" {
			if _, ok := indexByID[e.RevokesEventID]; ok {
				visit(e.RevokesEventID, stack)
			}
		}
		stack[id] = false
		placed[id] = true
		position[id] = len(order)
		order = append(order, id)
	}
	for _, e := range events {
		visit(e.EventID, map[string]bool{})
	}

	sort.SliceStable(events, func(i, j int) bool {
		return position[events[i].EventID] < position[events[j].EventID]
	})
	return events
}

// Project deterministically builds the canonical view from an ordered event
// chain. It is pure and therefore stably replayable.
//
// Consent scopes are treated as tombstones: every granted scope is collected,
// every revoked scope is collected, and the effective set is
// granted - revoked. A scope that was once revoked can therefore never be
// resurrected by a later (or retried) event that re-grants it, while scopes
// that were never revoked remain intact. This is what keeps a late hotline
// retry from re-exposing a phone number whose CONTACT_CALLBACK consent was
// already withdrawn.
func Project(requestID string, events []*AppliedEvent) *CanonicalView {
	ordered := projectionOrder(events)

	granted := map[string]bool{}
	revoked := map[string]bool{}
	accs := map[string]bool{}
	sourceRefs := map[string]string{}
	var person Person
	var callback *int
	var summary string
	var lastSeq int64
	var updatedAt time.Time
	chain := make([]string, 0, len(ordered))

	for _, e := range ordered {
		chain = append(chain, e.EventID)
		if e.Sequence > lastSeq {
			lastSeq = e.Sequence
		}
		if e.CreatedAt.After(updatedAt) {
			updatedAt = e.CreatedAt
		}
		mergePerson(&person, e.Person)
		if e.SourceID != "" {
			sourceRefs[e.Channel] = e.SourceID
		}
		for _, a := range e.Accommodations {
			accs[a] = true
		}
		for _, s := range e.Consent {
			granted[s] = true
		}
		for _, s := range e.RevocationScopes {
			revoked[s] = true
			delete(granted, s)
		}
	}

	// Tombstone rule: a revoked scope is permanently withdrawn, even if another
	// event in the chain (including a late retried delivery) also grants it.
	effective := map[string]bool{}
	for s := range granted {
		if !revoked[s] {
			effective[s] = true
		}
	}

	contactOK := effective["CONTACT_CALLBACK"]
	accTransfer := effective["ACCOMMODATION_TRANSFER"]
	summaryTransfer := effective["CASE_SUMMARY_TRANSFER"]

	viewPerson := person
	if !contactOK {
		viewPerson.Phone = ""
		viewPerson.Email = ""
		callback = nil
	}

	accList := make([]string, 0, len(accs))
	for a := range accs {
		accList = append(accList, a)
	}
	sort.Strings(accList)
	if !accTransfer {
		accList = []string{}
	}

	consentList := make([]string, 0, len(effective))
	for s := range effective {
		consentList = append(consentList, s)
	}
	sort.Strings(consentList)

	if !summaryTransfer {
		summary = ""
	}

	v := &CanonicalView{
		RequestID:                 requestID,
		Person:                    viewPerson,
		ContactAvailable:          contactOK,
		Accommodations:            accList,
		ConsentScopes:             consentList,
		SourceReferences:          sourceRefs,
		CallbackWindowMinutes:     callback,
		Summary:                   summary,
		SummaryTransferable:       summaryTransfer,
		AccommodationTransferable: accTransfer,
		EventChain:                chain,
		Sequence:                  lastSeq,
		UpdatedAt:                 updatedAt,
	}
	v.ProjectionHash = stableHash(v)
	return v
}
