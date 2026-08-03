package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/accessible-intake/gateway/internal/contracts"
)

// Normalize resolves channel-native field names into a NormalizedEvent.
// It does not validate codes; that is performed by Validate so callers can
// collect per-item errors.
func Normalize(e EventEnvelope) NormalizedEvent {
	n := NormalizedEvent{
		EventID:            e.EventID,
		Channel:            e.Channel,
		CanonicalRequestID: e.CanonicalRequestID,
		Revokes:            e.Revokes,
		RevocationScopes:   append([]string(nil), e.Scopes...),
	}

	switch e.Channel {
	case ChannelPhysical:
		n.SourceID = e.DeskReceiptNo
		if e.Visitor != nil {
			n.Person = *e.Visitor
		}
	case ChannelHotline:
		n.SourceID = e.CallRef
		if e.Caller != nil {
			n.Person = *e.Caller
		}
		n.CallbackWindow = e.CallbackWindowMinutes
	case ChannelWeb:
		n.SourceID = e.SubmissionID
		if e.Applicant != nil {
			n.Person = *e.Applicant
		}
	}

	// The normalized person alias overrides/merges on every channel.
	if e.Person != nil {
		n.Person = *e.Person
	}

	n.Accommodations = append([]string(nil), e.Accommodations...)
	n.Consent = append([]string(nil), e.Consent...)
	return n
}

// IsRevocation reports whether the event is a revocation.
func (n NormalizedEvent) IsRevocation() bool {
	return n.Revokes != "" || len(n.RevocationScopes) > 0
}

// SubjectKey derives a stable linkage key from the person identity. The same
// applicant reporting through different channels produces the same key when
// they share a reference number, phone, or email (first non-empty wins).
// If no identifier is present the key is derived from the trimmed full name.
func (n NormalizedEvent) SubjectKey() string {
	p := n.Person
	switch {
	case strings.TrimSpace(p.ReferenceNumber) != "":
		return "ref:" + strings.ToLower(strings.TrimSpace(p.ReferenceNumber))
	case strings.TrimSpace(p.Phone) != "":
		return "tel:" + normalizeDigits(p.Phone)
	case strings.TrimSpace(p.Email) != "":
		return "mailto:" + strings.ToLower(strings.TrimSpace(p.Email))
	case strings.TrimSpace(p.FullName) != "":
		return "name:" + strings.ToLower(strings.Join(strings.Fields(p.FullName), " "))
	default:
		return ""
	}
}

func normalizeDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// StablePayloadHash returns a deterministic SHA-256 of the canonical payload
// so that same eventId + same bytes always maps to the same hash, regardless
// of JSON key ordering.
func StablePayloadHash(e EventEnvelope) string {
	// Marshal through a generic structure then sort keys for stability.
	b, _ := json.Marshal(e)
	var any any
	_ = json.Unmarshal(b, &any)
	canonical := sortedJSON(any)
	out, _ := json.Marshal(canonical)
	sum := sha256.Sum256(out)
	return hex.EncodeToString(sum[:])
}

func sortedJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(t))
		for _, k := range keys {
			out[k] = sortedJSON(t[k])
		}
		return out
	case []any:
		for i := range t {
			t[i] = sortedJSON(t[i])
		}
		return t
	default:
		return v
	}
}

// Validate inspects a normalized event against the contracts fixture and
// returns one ItemResult per field/item. A fatal top-level error (missing
// eventId or unknown channel) returns fatal=true; per-item codes never set
// fatal so valid items can still be committed.
func Validate(n NormalizedEvent, c *contracts.Contracts) (results []ItemResult, fatal bool) {
	add := func(item, code, msg string) {
		results = append(results, ItemResult{Item: item, Code: code, Message: msg})
	}

	if strings.TrimSpace(n.EventID) == "" {
		add("eventId", ItemMissingField, "eventId is required")
		fatal = true
	}
	// A revocation is a control event (per the fixture it carries eventId,
	// revokes, and scopes but no channel). Non-revocation events must name a
	// declared channel.
	if !n.IsRevocation() && !c.IsChannel(n.Channel) {
		add("channel", ItemUnknownChannel, "unknown channel: "+n.Channel)
		fatal = true
	}
	if fatal {
		return results, true
	}

	if n.IsRevocation() {
		if strings.TrimSpace(n.Revokes) == "" {
			add("revokes", ItemMissingField, "revokes is required for revocation events")
			fatal = true
		}
		for i, s := range n.RevocationScopes {
			if !c.IsConsentScope(s) {
				add(itemName("scopes", i), ItemUnknownScope, "unknown consent scope: "+s)
			}
		}
		return results, fatal
	}

	ch := c.Channels[n.Channel]
	if strings.TrimSpace(n.SourceID) == "" {
		add(ch.SourceIDField, ItemMissingField, ch.SourceIDField+" is required for channel "+n.Channel)
	}

	for i, code := range n.Accommodations {
		if !c.IsAccommodation(code) {
			add(itemName("accommodations", i), ItemUnknownCode, "unknown accommodation code: "+code)
		}
	}
	for i, scope := range n.Consent {
		if !c.IsConsentScope(scope) {
			add(itemName("consent", i), ItemUnknownScope, "unknown consent scope: "+scope)
		}
	}
	if n.CallbackWindow != nil {
		windowStatus, _, result := ClassifyCallbackWindow(n.CallbackWindow)
		switch windowStatus {
		case WindowNegativeRejected:
			add(result.Item, result.Code, result.Message)
		case WindowCrossDayRejected:
			add(result.Item, result.Code, result.Message)
		case WindowZero, WindowOK:
			// Valid: zero means ASAP, positive within 24h is accepted.
		}
	}
	return results, false
}

func itemName(field string, idx int) string {
	b := strings.Builder{}
	b.WriteString(field)
	b.WriteByte('[')
	b.WriteString(itoa(idx))
	b.WriteByte(']')
	return b.String()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
