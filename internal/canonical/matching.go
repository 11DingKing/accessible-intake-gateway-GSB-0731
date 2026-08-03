package canonical

import (
	"sort"
	"strings"
)

// IdentityOf extracts the normalized identity fragment from a normalized
// event. Phone is reduced to digits, email is lower-cased and trimmed, and
// name is collapsed/lowered for comparison.
func (n NormalizedEvent) IdentityOf() IdentityFragment {
	p := n.Person
	return IdentityFragment{
		ReferenceNumber: strings.TrimSpace(p.ReferenceNumber),
		PhoneDigits:     normalizeDigits(p.Phone),
		Email:           strings.ToLower(strings.TrimSpace(p.Email)),
		FullNameNorm:    normalizeName(p.FullName),
	}
}

func normalizeName(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// ScoredCandidate is a canonical request evaluated against an incoming
// identity fragment.
type ScoredCandidate struct {
	CanonicalID string
	Confidence  string
	Score       int
	Reasons     []MatchEvidence
	Conflicts   []MatchEvidence
}

// HasAnyMatch reports whether at least one identifier matched.
func (s ScoredCandidate) HasAnyMatch() bool { return len(s.Reasons) > 0 }

// HasConflict reports whether a present field on both sides differed.
func (s ScoredCandidate) HasConflict() bool { return len(s.Conflicts) > 0 }

// ScoreCandidate compares an incoming identity fragment against an existing
// canonical identity fragment and returns a deterministic score with reasons
// and conflicts. The scoring is pure and stable.
//
// Weights:
//   reference number exact: 100 (strongest organizational identifier)
//   phone digits exact:      80
//   email exact:             80
//   normalized name exact:   10 (weak; never auto-links on its own)
//
// When a field is present on both sides but differs, it is recorded as a
// conflict and reduces confidence from HIGH to MEDIUM.
func ScoreCandidate(incoming, existing IdentityFragment) ScoredCandidate {
	c := ScoredCandidate{}
	addReason := func(field, reason, conf, detail string, weight int) {
		c.Reasons = append(c.Reasons, MatchEvidence{
			Field: field, Reason: reason, Confidence: conf, Detail: detail,
		})
		c.Score += weight
	}
	addConflict := func(field, detail string) {
		c.Conflicts = append(c.Conflicts, MatchEvidence{
			Field: field, Reason: ReasonIdentityConflict,
			Confidence: ConfidenceHigh, Detail: detail,
		})
	}

	if incoming.ReferenceNumber != "" && existing.ReferenceNumber != "" {
		if incoming.ReferenceNumber == existing.ReferenceNumber {
			addReason("referenceNumber", ReasonReferenceExact, ConfidenceHigh, "exact", 100)
		} else {
			addConflict("referenceNumber", "referenceNumber present on both sides but differs")
		}
	}
	if incoming.PhoneDigits != "" && existing.PhoneDigits != "" {
		if incoming.PhoneDigits == existing.PhoneDigits {
			addReason("phone", ReasonPhoneExact, ConfidenceHigh, "digits-exact", 80)
		} else {
			addConflict("phone", "phone present on both sides but differs")
		}
	}
	if incoming.Email != "" && existing.Email != "" {
		if incoming.Email == existing.Email {
			addReason("email", ReasonEmailExact, ConfidenceHigh, "case-insensitive-exact", 80)
		} else {
			addConflict("email", "email present on both sides but differs")
		}
	}
	if incoming.FullNameNorm != "" && existing.FullNameNorm != "" {
		if incoming.FullNameNorm == existing.FullNameNorm {
			addReason("fullName", ReasonNameExact, ConfidenceLow, "normalized-exact", 10)
		}
	}

	switch {
	case c.Score >= 100:
		c.Confidence = ConfidenceHigh
	case c.Score >= 80:
		if c.HasConflict() {
			c.Confidence = ConfidenceMedium
		} else {
			c.Confidence = ConfidenceHigh
		}
	case c.Score >= 10:
		c.Confidence = ConfidenceLow
	default:
		c.Confidence = ConfidenceNone
	}
	return c
}

// CanAutoLink reports whether the score is strong enough to attach the event
// to an existing chain automatically. Name-only matches (score 10) do NOT
// auto-link because names are not unique.
func (s ScoredCandidate) CanAutoLink() bool {
	return s.Score >= 80
}

// RankCandidates sorts scored candidates deterministically: highest score
// first, then highest confidence (HIGH>MEDIUM>LOW), then fewest conflicts,
// then lexicographically smallest canonical id as a final tiebreaker. The
// first element after sorting is the selected candidate.
func RankCandidates(cands []ScoredCandidate) []ScoredCandidate {
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		ar := confidenceRank(a.Confidence)
		br := confidenceRank(b.Confidence)
		if ar != br {
			return ar > br
		}
		if len(a.Conflicts) != len(b.Conflicts) {
			return len(a.Conflicts) < len(b.Conflicts)
		}
		return a.CanonicalID < b.CanonicalID
	})
	return cands
}

func confidenceRank(c string) int {
	switch c {
	case ConfidenceHigh:
		return 3
	case ConfidenceMedium:
		return 2
	case ConfidenceLow:
		return 1
	default:
		return 0
	}
}

// ClassifyCallbackWindow categorizes a callbackWindowMinutes value.
//   nil  -> ABSENT (no window requested)
//   0    -> ZERO (ASAP / no constraint) — valid
//   <0   -> NEGATIVE_REJECTED — per-item INVALID_VALUE
//   >=1440 -> CROSS_DAY_REJECTED — per-item CROSS_DAY_WINDOW
//   1..1439 -> OK
func ClassifyCallbackWindow(w *int) (status string, effective *int, result ItemResult) {
	if w == nil {
		return WindowAbsent, nil, ItemResult{}
	}
	v := *w
	switch {
	case v == 0:
		zero := 0
		return WindowZero, &zero, ItemResult{}
	case v < 0:
		return WindowNegativeRejected, nil, ItemResult{
			Item: "callbackWindowMinutes", Code: ItemInvalidValue,
			Message: "callbackWindowMinutes must be zero or positive",
		}
	case v >= MaxCallbackWindowMinutes:
		return WindowCrossDayRejected, nil, ItemResult{
			Item: "callbackWindowMinutes", Code: ItemCrossDayWindow,
			Message: "callbackWindowMinutes must be less than 1440 (24h); cross-day windows are not accepted",
		}
	default:
		cp := v
		return WindowOK, &cp, ItemResult{}
	}
}

// ToMatchCandidates converts internal scored candidates into the API-facing
// MatchCandidate slice, marking the selected one.
func ToMatchCandidates(scored []ScoredCandidate, selectedID string) []MatchCandidate {
	out := make([]MatchCandidate, 0, len(scored))
	for i, s := range scored {
		reasons := s.Reasons
		if reasons == nil {
			reasons = []MatchEvidence{}
		}
		conflicts := s.Conflicts
		if conflicts == nil {
			conflicts = []MatchEvidence{}
		}
		out = append(out, MatchCandidate{
			CanonicalRequestID: s.CanonicalID,
			Rank:               i + 1,
			Selected:           s.CanonicalID == selectedID,
			Confidence:         s.Confidence,
			Score:              s.Score,
			Reasons:            reasons,
			Conflicts:          conflicts,
		})
	}
	return out
}
