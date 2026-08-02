package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Event types.
const (
	EventTypeIntake     = "INTAKE"
	EventTypeRevocation = "REVOCATION"
)

// Record result statuses.
const (
	StatusAccepted = "ACCEPTED"
	StatusReplayed = "REPLAYED"
	StatusConflict = "CONFLICT"
	StatusFailed   = "FAILED"
)

// Attempt outcomes recorded in the attempts log.
const (
	AttemptAccepted         = "ACCEPTED"
	AttemptReplayed         = "REPLAYED"
	AttemptConflict         = "CONFLICT"
	AttemptValidationFailed = "VALIDATION_FAILED"
	AttemptTransientFailure = "TRANSIENT_FAILURE"
)

// Validation / processing error codes.
const (
	CodeInvalidJSON            = "INVALID_JSON"
	CodeInvalidEnvelope        = "INVALID_ENVELOPE"
	CodeFieldTypeMismatch      = "FIELD_TYPE_MISMATCH"
	CodeMissingEventID         = "MISSING_EVENT_ID"
	CodeInvalidEventType       = "INVALID_EVENT_TYPE"
	CodeMissingChannel         = "MISSING_CHANNEL"
	CodeUnknownChannel         = "UNKNOWN_CHANNEL"
	CodeMissingCorrelationID   = "MISSING_CORRELATION_ID"
	CodeInvalidPerson          = "INVALID_PERSON"
	CodeUnknownAccommodation   = "UNKNOWN_ACCOMMODATION"
	CodeUnknownConsentScope    = "UNKNOWN_CONSENT_SCOPE"
	CodeInvalidCallbackWindow  = "INVALID_CALLBACK_WINDOW"
	CodeInvalidFieldForChannel = "INVALID_FIELD_FOR_CHANNEL"
	CodeMissingRevokes         = "MISSING_REVOKES"
	CodeMissingScopes          = "MISSING_SCOPES"
	CodeRevokesUnknownEvent    = "REVOKES_UNKNOWN_EVENT"
	CodePayloadConflict        = "PAYLOAD_CONFLICT"
	CodeTransientFailure       = "TRANSIENT_FAILURE"
	CodeNotFound               = "NOT_FOUND"
)

// FieldError is one machine-readable validation failure on a record.
type FieldError struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (e FieldError) Error() string { return e.Code + ": " + e.Message }

// RecordResult is the per-record outcome of submitting one envelope.
// It is stored next to the accepted event so byte-identical retries can be
// answered with the original result.
type RecordResult struct {
	EventID          string       `json:"eventId"`
	Status           string       `json:"status"`
	RequestID        string       `json:"requestId,omitempty"`
	Seq              int64        `json:"seq,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay,omitempty"`
	Retryable        bool         `json:"retryable,omitempty"`
	AppliedScopes    []string     `json:"appliedScopes,omitempty"`
	EffectiveConsent []string     `json:"effectiveConsent,omitempty"`
	Errors           []FieldError `json:"errors,omitempty"`
}

// Person is the normalized applicant identity shared by all channels.
type Person struct {
	Name     string `json:"name"`
	Phone    string `json:"phone,omitempty"`
	Email    string `json:"email,omitempty"`
	IDNumber string `json:"idNumber,omitempty"`
}

// Envelope is a parsed channel event ready for normalization.
type Envelope struct {
	EventID               string
	EventType             string
	Channel               string
	CorrelationField      string
	CorrelationID         string
	Person                *Person
	Accommodations        []string
	Consent               []string
	CallbackWindowMinutes *int64
	Revokes               string
	Scopes                []string
	Raw                   json.RawMessage
}

// ChannelLink is the per-channel source correlation preserved on a request.
type ChannelLink struct {
	CorrelationField      string   `json:"correlationField"`
	CorrelationID         string   `json:"correlationId"`
	EventIDs              []string `json:"eventIds"`
	CallbackWindowMinutes *int64   `json:"callbackWindowMinutes,omitempty"`
}

// ConsentState tracks which event granted or revoked each scope, so a
// revocation removes exactly the grant it targets and retries never
// resurrect a revoked scope.
type ConsentState struct {
	Granted map[string][]string `json:"granted"` // scope -> granting event IDs
	Revoked map[string][]string `json:"revoked"` // scope -> revoking event IDs
}

// RequestState is the folded canonical state of one request.
type RequestState struct {
	CanonicalVersion      string                  `json:"canonicalVersion"`
	Channels              map[string]*ChannelLink `json:"channels"`
	Accommodations        []string                `json:"accommodations"`
	Consent               ConsentState            `json:"consent"`
	CallbackWindowMinutes *int64                  `json:"callbackWindowMinutes,omitempty"`
}

func newRequestState(version string) *RequestState {
	return &RequestState{
		CanonicalVersion: version,
		Channels:         map[string]*ChannelLink{},
		Accommodations:   []string{},
		Consent:          ConsentState{Granted: map[string][]string{}, Revoked: map[string][]string{}},
	}
}

// EffectiveConsent returns scopes with at least one live grant, sorted.
func (s *RequestState) EffectiveConsent() []string {
	out := []string{}
	for scope, grantors := range s.Consent.Granted {
		if len(grantors) > 0 {
			out = append(out, scope)
		}
	}
	sort.Strings(out)
	return out
}

// RevokedScopes returns scopes that have at least one revocation recorded.
func (s *RequestState) RevokedScopes() []string {
	out := []string{}
	for scope, revokers := range s.Consent.Revoked {
		if len(revokers) > 0 {
			out = append(out, scope)
		}
	}
	sort.Strings(out)
	return out
}

// applyIntake folds one accepted INTAKE event into the state.
func (s *RequestState) applyIntake(env *Envelope) {
	link, ok := s.Channels[env.Channel]
	if !ok {
		link = &ChannelLink{EventIDs: []string{}}
		s.Channels[env.Channel] = link
	}
	link.CorrelationField = env.CorrelationField
	link.CorrelationID = env.CorrelationID
	link.EventIDs = appendUnique(link.EventIDs, env.EventID)
	if env.CallbackWindowMinutes != nil {
		v := *env.CallbackWindowMinutes
		link.CallbackWindowMinutes = &v
		s.CallbackWindowMinutes = &v
	}
	for _, code := range env.Accommodations {
		s.Accommodations = appendUnique(s.Accommodations, code)
	}
	sort.Strings(s.Accommodations)
	for _, scope := range env.Consent {
		s.Consent.Granted[scope] = appendUnique(s.Consent.Granted[scope], env.EventID)
	}
}

// applyRevocation folds one accepted REVOCATION event into the state and
// returns the scopes it actually removed (a scope is only removed when the
// referenced event had granted it).
func (s *RequestState) applyRevocation(env *Envelope) []string {
	applied := []string{}
	for _, scope := range env.Scopes {
		grantors := s.Consent.Granted[scope]
		if !contains(grantors, env.Revokes) {
			continue
		}
		s.Consent.Granted[scope] = removeString(grantors, env.Revokes)
		s.Consent.Revoked[scope] = appendUnique(s.Consent.Revoked[scope], env.EventID)
		applied = append(applied, scope)
	}
	sort.Strings(applied)
	return applied
}

func appendUnique(list []string, v string) []string {
	if contains(list, v) {
		return list
	}
	return append(list, v)
}

func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

func removeString(list []string, v string) []string {
	out := list[:0]
	for _, item := range list {
		if item != v {
			out = append(out, item)
		}
	}
	return out
}

// CanonicalJSON re-encodes raw JSON with sorted object keys and preserved
// number literals, producing a stable byte form for hashing and replay.
func CanonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// PayloadHash returns the SHA-256 hex digest of the canonical payload form.
func PayloadHash(raw []byte) (string, error) {
	canon, err := CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// personMergeKey derives the cross-channel merge key for an envelope.
// Events without a usable person identity can never be merged safely, so
// they anchor a standalone request keyed by their own event ID.
func personMergeKey(env *Envelope) string {
	if env.Person == nil {
		return "event:" + env.EventID
	}
	name := normalizeName(env.Person.Name)
	var material string
	switch {
	case env.Person.IDNumber != "":
		material = "id:" + strings.ToUpper(strings.TrimSpace(env.Person.IDNumber))
	case env.Person.Phone != "":
		material = "phone:" + digitsOnly(env.Person.Phone)
	default:
		material = "email:" + strings.ToLower(strings.TrimSpace(env.Person.Email))
	}
	sum := sha256.Sum256([]byte("v1|" + name + "|" + material))
	return "person:" + hex.EncodeToString(sum[:])
}

func normalizeName(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// parseEnvelope decodes and validates raw JSON against the registry.
// All field-level problems are collected so one record reports every error.
func parseEnvelope(reg *Registry, raw json.RawMessage) (*Envelope, []FieldError) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, []FieldError{{Code: CodeInvalidJSON, Message: "record is not a JSON object: " + err.Error()}}
	}
	var errs []FieldError
	env := &Envelope{Raw: raw}

	strField := func(name string) (string, bool) {
		rawF, ok := fields[name]
		if !ok || string(rawF) == "null" {
			return "", false
		}
		var s string
		if err := json.Unmarshal(rawF, &s); err != nil {
			errs = append(errs, FieldError{Code: CodeFieldTypeMismatch, Field: name, Message: fmt.Sprintf("field %q must be a string", name)})
			return "", false
		}
		return s, true
	}
	strListField := func(name string) ([]string, bool) {
		rawF, ok := fields[name]
		if !ok || string(rawF) == "null" {
			return nil, false
		}
		var list []string
		if err := json.Unmarshal(rawF, &list); err != nil {
			errs = append(errs, FieldError{Code: CodeFieldTypeMismatch, Field: name, Message: fmt.Sprintf("field %q must be an array of strings", name)})
			return nil, false
		}
		return list, true
	}

	// eventId: the idempotency key.
	if id, present := strField("eventId"); present {
		env.EventID = strings.TrimSpace(id)
	}
	if env.EventID == "" && !hasErrorFor(errs, "eventId") {
		errs = append(errs, FieldError{Code: CodeMissingEventID, Field: "eventId", Message: "eventId is required"})
	}

	// eventType: explicit, or implied by the presence of "revokes".
	explicitType, typePresent := strField("eventType")
	_, revokesPresent := fields["revokes"]
	switch {
	case typePresent && explicitType != "":
		switch explicitType {
		case EventTypeIntake:
			if revokesPresent {
				errs = append(errs, FieldError{Code: CodeInvalidEnvelope, Field: "revokes", Message: "revokes is not allowed on an INTAKE event"})
			}
			env.EventType = EventTypeIntake
		case EventTypeRevocation:
			env.EventType = EventTypeRevocation
		default:
			errs = append(errs, FieldError{Code: CodeInvalidEventType, Field: "eventType", Message: "eventType must be INTAKE or REVOCATION"})
		}
	case revokesPresent:
		env.EventType = EventTypeRevocation
	default:
		env.EventType = EventTypeIntake
	}

	channel, channelPresent := strField("channel")
	env.Channel = channel
	var contract ChannelContract
	var channelKnown bool
	if channelPresent && channel != "" {
		contract, channelKnown = reg.Channel(channel)
		if !channelKnown {
			errs = append(errs, FieldError{Code: CodeUnknownChannel, Field: "channel", Message: fmt.Sprintf("unknown channel %q; registered: %s", channel, strings.Join(reg.ChannelIDs(), ", "))})
		}
	}

	if env.EventType == EventTypeIntake {
		if env.Channel == "" && !hasErrorFor(errs, "channel") {
			errs = append(errs, FieldError{Code: CodeMissingChannel, Field: "channel", Message: "channel is required for INTAKE events"})
		}
		if channelKnown {
			env.CorrelationField = contract.SourceIDField
			corr, corrPresent := strField(contract.SourceIDField)
			if !corrPresent || strings.TrimSpace(corr) == "" {
				if !hasErrorFor(errs, contract.SourceIDField) {
					errs = append(errs, FieldError{Code: CodeMissingCorrelationID, Field: contract.SourceIDField, Message: fmt.Sprintf("source correlation field %q is required for channel %s", contract.SourceIDField, channel)})
				}
			} else {
				env.CorrelationID = strings.TrimSpace(corr)
			}
			env.Person = parsePerson(fields, contract.PersonField, &errs)
		}
		if list, ok := strListField("accommodations"); ok {
			for _, code := range list {
				if !reg.KnownAccommodation(code) {
					errs = append(errs, FieldError{Code: CodeUnknownAccommodation, Field: "accommodations", Message: fmt.Sprintf("unknown accommodation code %q", code)})
					continue
				}
				env.Accommodations = appendUnique(env.Accommodations, code)
			}
		}
		if list, ok := strListField("consent"); ok {
			for _, scope := range list {
				if !reg.KnownConsentScope(scope) {
					errs = append(errs, FieldError{Code: CodeUnknownConsentScope, Field: "consent", Message: fmt.Sprintf("unknown consent scope %q", scope)})
					continue
				}
				env.Consent = appendUnique(env.Consent, scope)
			}
		}
		env.parseCallbackWindow(reg, fields, channelKnown, contract, &errs)
	} else if env.EventType == EventTypeRevocation {
		revokes, ok := strField("revokes")
		if !ok || strings.TrimSpace(revokes) == "" {
			if !hasErrorFor(errs, "revokes") {
				errs = append(errs, FieldError{Code: CodeMissingRevokes, Field: "revokes", Message: "revokes is required for REVOCATION events"})
			}
		} else {
			env.Revokes = strings.TrimSpace(revokes)
		}
		scopes, scopesPresent := strListField("scopes")
		if !scopesPresent || len(scopes) == 0 {
			if !hasErrorFor(errs, "scopes") {
				errs = append(errs, FieldError{Code: CodeMissingScopes, Field: "scopes", Message: "scopes is required and must be non-empty for REVOCATION events"})
			}
		}
		for _, scope := range scopes {
			if !reg.KnownConsentScope(scope) {
				errs = append(errs, FieldError{Code: CodeUnknownConsentScope, Field: "scopes", Message: fmt.Sprintf("unknown consent scope %q", scope)})
				continue
			}
			env.Scopes = appendUnique(env.Scopes, scope)
		}
	}

	if len(errs) > 0 {
		return env, errs
	}
	return env, nil
}

// parseCallbackWindow validates the minute-based callback field declared by
// the channel contract. It must be a non-negative JSON integer, and it is
// rejected on channels whose contract does not declare it.
func (e *Envelope) parseCallbackWindow(reg *Registry, fields map[string]json.RawMessage, channelKnown bool, contract ChannelContract, errs *[]FieldError) {
	rawF, present := fields["callbackWindowMinutes"]
	if !present || string(rawF) == "null" {
		return
	}
	if !channelKnown || contract.CallbackWindowField == "" {
		*errs = append(*errs, FieldError{Code: CodeInvalidFieldForChannel, Field: "callbackWindowMinutes", Message: fmt.Sprintf("callbackWindowMinutes is not declared for channel %q", e.Channel)})
		return
	}
	var num json.Number
	dec := json.NewDecoder(bytes.NewReader(rawF))
	dec.UseNumber()
	if err := dec.Decode(&num); err != nil {
		*errs = append(*errs, FieldError{Code: CodeInvalidCallbackWindow, Field: "callbackWindowMinutes", Message: "callbackWindowMinutes must be a number of minutes"})
		return
	}
	minutes, err := num.Int64()
	if err != nil {
		*errs = append(*errs, FieldError{Code: CodeInvalidCallbackWindow, Field: "callbackWindowMinutes", Message: "callbackWindowMinutes must be a whole number of minutes"})
		return
	}
	if minutes < 0 {
		*errs = append(*errs, FieldError{Code: CodeInvalidCallbackWindow, Field: "callbackWindowMinutes", Message: "callbackWindowMinutes must not be negative"})
		return
	}
	e.CallbackWindowMinutes = &minutes
}

// parsePerson validates the channel's person field. A person object is
// optional, but when present it must carry a name and at least one contact
// or ID attribute so cross-channel merging never guesses.
func parsePerson(fields map[string]json.RawMessage, personField string, errs *[]FieldError) *Person {
	rawF, present := fields[personField]
	if !present || string(rawF) == "null" {
		return nil
	}
	var p Person
	if err := json.Unmarshal(rawF, &p); err != nil {
		*errs = append(*errs, FieldError{Code: CodeInvalidPerson, Field: personField, Message: fmt.Sprintf("field %q must be an object with name/phone/email/idNumber", personField)})
		return nil
	}
	p.Name = strings.TrimSpace(p.Name)
	if p.Name == "" {
		*errs = append(*errs, FieldError{Code: CodeInvalidPerson, Field: personField + ".name", Message: "person name is required when the person object is present"})
	}
	if p.Phone == "" && p.Email == "" && p.IDNumber == "" {
		*errs = append(*errs, FieldError{Code: CodeInvalidPerson, Field: personField, Message: "person requires at least one of phone, email or idNumber"})
	}
	return &p
}

func hasErrorFor(errs []FieldError, field string) bool {
	for _, e := range errs {
		if e.Field == field {
			return true
		}
	}
	return false
}
