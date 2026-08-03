// Package normalize turns raw per-channel intake envelopes into channel-neutral
// canonical events. It is pure and deterministic: the same bytes always yield
// the same canonical event and the same payload hash, which is what makes
// idempotency and replay stable.
package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/accessible-intake-gateway/internal/contract"
	"github.com/accessible-intake-gateway/internal/identity"
	"github.com/accessible-intake-gateway/internal/model"
)

// Error codes for per-record validation failures.
const (
	CodeMalformed     = "MALFORMED"
	CodeMissingField  = "MISSING_FIELD"
	CodeUnknownValue  = "UNKNOWN_VALUE"
	CodeInvalidValue  = "INVALID_VALUE"
	CodeUnknownChannel = "UNKNOWN_CHANNEL"
)

// Normalize converts a single raw envelope into a canonical event. It always
// returns an event shell (so callers can record an outcome keyed by eventId)
// alongside any field-scoped validation errors. A nil error slice means the
// event is structurally valid and ready to apply.
func Normalize(c *contract.Contract, raw []byte) (*model.CanonicalEvent, []model.RecordError) {
	var errs []model.RecordError
	add := func(field, code, msg string) {
		errs = append(errs, model.RecordError{Field: field, Code: code, Message: msg})
	}

	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return &model.CanonicalEvent{PayloadHash: payloadHash(raw)}, []model.RecordError{
			{Field: "_", Code: CodeMalformed, Message: fmt.Sprintf("envelope is not a JSON object: %v", err)},
		}
	}

	ev := &model.CanonicalEvent{PayloadHash: payloadHash(raw)}
	ev.EventID = decodeString(fields["eventId"])
	if ev.EventID == "" {
		add("eventId", CodeMissingField, "eventId is required")
	}

	// A revocation is any envelope that references another event via "revokes".
	if rawRevokes, ok := fields["revokes"]; ok {
		ev.Kind = model.KindRevocation
		ev.RevokesEventID = decodeString(rawRevokes)
		if ev.RevokesEventID == "" {
			add("revokes", CodeMissingField, "revokes must reference a prior eventId")
		}
		scopes, ok := decodeStringSlice(fields["scopes"])
		if !ok {
			add("scopes", CodeInvalidValue, "scopes must be an array of strings")
		}
		if len(scopes) == 0 {
			add("scopes", CodeMissingField, "revocation must list at least one scope")
		}
		for _, s := range scopes {
			if !c.KnownConsentScope(s) {
				add("scopes", CodeUnknownValue, fmt.Sprintf("unknown consent scope %q", s))
			}
		}
		ev.ConsentRevokes = scopes
		// Channel is optional on a revocation; record it if supplied and known.
		if ch := decodeString(fields["channel"]); ch != "" {
			if _, known := c.Channel(ch); !known {
				add("channel", CodeUnknownChannel, fmt.Sprintf("unknown channel %q", ch))
			}
			ev.Channel = ch
		}
		// A revocation must correlate to the same canonical request it withdraws
		// consent from. Fall back to the referenced event id when no explicit
		// correlation number is supplied.
		ev.CanonicalKey = decodeString(fields["canonicalKey"])
		if ev.CanonicalKey == "" {
			ev.CanonicalKey = ev.RevokesEventID
		}
		return ev, errs
	}

	ev.Kind = model.KindStandard
	ev.Channel = decodeString(fields["channel"])
	if ev.Channel == "" {
		add("channel", CodeMissingField, "channel is required")
		return ev, errs
	}
	ch, known := c.Channel(ev.Channel)
	if !known {
		add("channel", CodeUnknownChannel, fmt.Sprintf("unknown channel %q", ev.Channel))
		return ev, errs
	}

	ev.SourceIDField = ch.SourceIDField
	ev.SourceID = decodeString(fields[ch.SourceIDField])
	if ev.SourceID == "" {
		add(ch.SourceIDField, CodeMissingField, fmt.Sprintf("%s is required for channel %s", ch.SourceIDField, ev.Channel))
	}

	ev.PersonField = ch.PersonField
	if person, ok := fields[ch.PersonField]; ok {
		ev.Person = append(json.RawMessage(nil), person...)
	}

	// Accommodations: every code must be recognized by the contract.
	if rawAcc, ok := fields["accommodations"]; ok {
		acc, ok := decodeStringSlice(rawAcc)
		if !ok {
			add("accommodations", CodeInvalidValue, "accommodations must be an array of strings")
		}
		for _, a := range acc {
			if !c.KnownAccommodation(a) {
				add("accommodations", CodeUnknownValue, fmt.Sprintf("unknown accommodation code %q", a))
			}
		}
		ev.Accommodations = acc
	}

	// Consent grants: every scope must be recognized by the contract.
	if rawConsent, ok := fields["consent"]; ok {
		grants, ok := decodeStringSlice(rawConsent)
		if !ok {
			add("consent", CodeInvalidValue, "consent must be an array of strings")
		}
		for _, g := range grants {
			if !c.KnownConsentScope(g) {
				add("consent", CodeUnknownValue, fmt.Sprintf("unknown consent scope %q", g))
			}
		}
		ev.ConsentGrants = grants
	}

	// Minute-based callback window (hotline). Preserve the contract's field
	// name and minute unit established in Round 1. Zero is valid (call back
	// immediately); negative is rejected; large values spanning a day are valid
	// but classified distinctly downstream.
	if rawWin, ok := fields[ch.CallbackWindowField]; ok && ch.CallbackWindowField != "" {
		var win int
		if err := json.Unmarshal(rawWin, &win); err != nil {
			add(ch.CallbackWindowField, CodeInvalidValue, "callback window must be an integer number of minutes")
		} else if win < 0 {
			add(ch.CallbackWindowField, CodeInvalidValue, "callback window minutes must not be negative")
		} else {
			ev.CallbackWindowMinutes = &win
		}
	}

	// Extract identity fragments (source correlation number + corroborating
	// signals) used for deterministic matching. Sensitive fragments are hashed
	// and their raw values held separately for consent-gated projection.
	extractFragments(ev, fields, add)

	// Canonical key correlates events across channels for one applicant. The
	// caller-supplied correlation number is authoritative when present; when
	// absent, the store resolves the request from corroborating fragments (see
	// store.resolveCanonicalKey), falling back to channel + source id only when
	// nothing matches. Leaving CanonicalKey empty here signals "resolve me".
	ev.CanonicalKey = decodeString(fields["canonicalKey"])

	return ev, errs
}

// extractFragments pulls identity fragments from the envelope. Recognized
// sources: the correlation number (canonicalKey/correlation) and an optional
// "identity" object with phone, email, dob, givenName, familyName, postalCode.
// Sensitive fragments (phone/email) contribute only a hash to the event's
// fragment list; their plaintext is stashed in RawContacts for the
// consent-gated contact projection and never enters evidence or the audit log.
func extractFragments(ev *model.CanonicalEvent, fields map[string]json.RawMessage, add func(field, code, msg string)) {
	seen := map[string]struct{}{}
	addFrag := func(fragType, value string) {
		if value == "" {
			return
		}
		spec, ok := identity.Lookup(fragType)
		if !ok {
			return
		}
		if _, dup := seen[fragType]; dup {
			return
		}
		seen[fragType] = struct{}{}
		frag := model.IdentityFragment{
			Type:      fragType,
			Hash:      identity.Hash(fragType, value),
			Unique:    spec.Unique,
			Sensitive: spec.Sensitive,
		}
		if spec.Sensitive {
			if ev.RawContacts == nil {
				ev.RawContacts = map[string]string{}
			}
			ev.RawContacts[fragType] = identity.Normalize(fragType, value)
		} else {
			frag.Value = identity.Normalize(fragType, value)
		}
		ev.Fragments = append(ev.Fragments, frag)
	}

	// The correlation number can be supplied at the top level as canonicalKey or
	// correlation; both denote the source correlation number.
	if v := decodeString(fields["canonicalKey"]); v != "" {
		addFrag(identity.FragCorrelation, v)
	} else if v := decodeString(fields["correlation"]); v != "" {
		addFrag(identity.FragCorrelation, v)
	}

	if rawID, ok := fields["identity"]; ok {
		var idObj map[string]json.RawMessage
		if err := json.Unmarshal(rawID, &idObj); err != nil {
			add("identity", CodeInvalidValue, "identity must be an object of fragments")
			return
		}
		for _, fragType := range []string{
			identity.FragPhone, identity.FragEmail, identity.FragDOB,
			identity.FragGivenName, identity.FragFamilyName, identity.FragPostalCode,
		} {
			if v := decodeString(idObj[fragType]); v != "" {
				addFrag(fragType, v)
			}
		}
	}
}

func decodeString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

func decodeStringSlice(raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 {
		return nil, true
	}
	var s []string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, false
	}
	return s, true
}

// payloadHash returns a deterministic digest of the envelope. The raw JSON is
// re-encoded in a canonical form (object keys sorted) so that byte-for-byte
// re-orderings of the same payload still hash identically, giving stable
// idempotency and replay.
func payloadHash(raw []byte) string {
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	canon := canonicalize(v)
	b, _ := json.Marshal(canon)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// canonicalize rewrites maps into a key-sorted, deterministic structure. Go's
// json encoder already sorts map[string]... keys, so returning the value is
// sufficient once nested maps use string keys.
func canonicalize(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]interface{}, len(t))
		for _, k := range keys {
			out[k] = canonicalize(t[k])
		}
		return out
	case []interface{}:
		for i := range t {
			t[i] = canonicalize(t[i])
		}
		return t
	default:
		return v
	}
}
