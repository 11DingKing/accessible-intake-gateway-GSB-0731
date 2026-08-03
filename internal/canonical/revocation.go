package canonical

// RevocableField identifies a PII field that can be cleared when its
// governing consent scope is revoked. Evidence that cannot be reverse-
// engineered (event id, channel, source correlation, payload hash, sequence,
// timestamps) is never affected.
const (
	FieldPhone          = "phone"
	FieldEmail          = "email"
	FieldFullName       = "full_name"
	FieldAccommodations = "accommodations"
)

// ScopeFields maps each consent scope to the reversible fields that must be
// cleared when that scope is revoked. The mapping is deterministic and is the
// single source of truth for "what does revoking this scope remove".
var ScopeFields = map[string][]string{
	"CONTACT_CALLBACK":        {FieldPhone, FieldEmail},
	"CASE_SUMMARY_TRANSFER":   {FieldFullName},
	"ACCOMMODATION_TRANSFER":  {FieldAccommodations},
}

// FieldsForScope returns the reversible fields governed by a scope. Unknown
// scopes return nil.
func FieldsForScope(scope string) []string {
	return ScopeFields[scope]
}

// IsAccommodationField reports whether the field is the accommodations
// pseudo-field (handled by deleting accommodation rows rather than clearing a
// scalar column).
func IsAccommodationField(f string) bool { return f == FieldAccommodations }
