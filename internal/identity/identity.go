// Package identity holds the deterministic identity-resolution policy used to
// match incoming intake events (notably out-of-order hotline callbacks) to an
// existing canonical request. It is pure: fragment normalization, hashing,
// callback-window classification, and confidence scoring depend only on their
// inputs, so a replay of the same events yields the same match reasons and
// confidence evidence.
package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
)

// Fragment types recognized for cross-channel identity matching. The source
// correlation number (the Round 1 canonicalKey / per-record correlation) is
// modeled as the decisive "correlation" fragment.
const (
	FragCorrelation = "correlation"
	FragPhone       = "phone"
	FragEmail       = "email"
	FragDOB         = "dob"
	FragFamilyName  = "familyName"
	FragGivenName   = "givenName"
	FragPostalCode  = "postalCode"
)

// Spec describes how a fragment type participates in matching.
type Spec struct {
	Type      string
	Weight    float64 // contribution to confidence when it matches
	Unique    bool    // a decisive identifier: differing values mean different requests
	Sensitive bool    // raw value is contact info; never exposed before consent
}

var registry = map[string]Spec{
	FragCorrelation: {FragCorrelation, 1.0, true, false},
	FragPhone:       {FragPhone, 0.5, false, true},
	FragEmail:       {FragEmail, 0.5, false, true},
	FragDOB:         {FragDOB, 0.4, false, false},
	FragFamilyName:  {FragFamilyName, 0.25, false, false},
	FragGivenName:   {FragGivenName, 0.2, false, false},
	FragPostalCode:  {FragPostalCode, 0.15, false, false},
}

// Lookup returns the spec for a fragment type and whether it is recognized.
func Lookup(fragType string) (Spec, bool) {
	s, ok := registry[fragType]
	return s, ok
}

var nonDigit = regexp.MustCompile(`[^0-9]`)

// Normalize canonicalizes a fragment value for stable, order-independent
// matching. Phones reduce to digits; emails/names lowercase and trim;
// correlation numbers only trim (they are case-significant references).
func Normalize(fragType, value string) string {
	v := strings.TrimSpace(value)
	switch fragType {
	case FragPhone:
		return nonDigit.ReplaceAllString(v, "")
	case FragEmail, FragFamilyName, FragGivenName:
		return strings.ToLower(v)
	default:
		return v
	}
}

// Hash returns a deterministic, domain-separated digest of a normalized
// fragment. Sensitive raw values are matched by hash so evidence and the index
// never need to carry the plaintext.
func Hash(fragType, value string) string {
	sum := sha256.Sum256([]byte(fragType + "\x00" + Normalize(fragType, value)))
	return hex.EncodeToString(sum[:])
}

// Confidence labels.
const (
	ConfidenceHigh   = "HIGH"
	ConfidenceMedium = "MEDIUM"
	ConfidenceLow    = "LOW"
	ConfidenceNone   = "NONE"
)

// Score sums the weights of the matched fragment types (capped at 1.0) and maps
// the total to a deterministic confidence label. A matched correlation is
// always decisive (HIGH).
func Score(matchedTypes []string) (string, float64) {
	total := 0.0
	correlated := false
	for _, t := range matchedTypes {
		if s, ok := registry[t]; ok {
			total += s.Weight
			if t == FragCorrelation {
				correlated = true
			}
		}
	}
	if total > 1.0 {
		total = 1.0
	}
	switch {
	case len(matchedTypes) == 0:
		return ConfidenceNone, 0
	case correlated || total >= 0.7:
		return ConfidenceHigh, total
	case total >= 0.4:
		return ConfidenceMedium, total
	default:
		return ConfidenceLow, total
	}
}

// Callback-window dispositions. The minute unit is unchanged from Round 1.
const (
	WindowImmediate = "IMMEDIATE" // 0 minutes: call back at once, no waiting window
	WindowIntraday  = "INTRADAY"  // 1..1439 minutes: resolves within the same day
	WindowCrossDay  = "CROSS_DAY" // >=1440 minutes: spans more than one calendar day
)

// MinutesPerDay is the intraday boundary for callback windows.
const MinutesPerDay = 1440

// ClassifyWindow maps a non-negative callback-window minute count to its
// disposition. Negative values are rejected upstream before this is called.
func ClassifyWindow(minutes int) string {
	switch {
	case minutes == 0:
		return WindowImmediate
	case minutes >= MinutesPerDay:
		return WindowCrossDay
	default:
		return WindowIntraday
	}
}

// SortTypes returns fragment types in a stable order for deterministic evidence.
func SortTypes(types []string) []string {
	out := append([]string(nil), types...)
	sort.Strings(out)
	return out
}
