package intake

import (
	"fmt"
	"sort"
	"strings"
)

const (
	ConfidenceExact  = "exact"
	ConfidenceHigh   = "high"
	ConfidenceMedium = "medium"
	ConfidenceLow    = "low"
	ConfidenceNone   = "none"

	MatchReasonExactIdentity    = "EXACT_IDENTITY"
	MatchReasonPhoneMatch       = "PHONE_MATCH"
	MatchReasonEmailMatch       = "EMAIL_MATCH"
	MatchReasonNameDOBPartial   = "NAME_DOB_PARTIAL_MATCH"
	MatchReasonCrossChannelLink = "CROSS_CHANNEL_LINK"
	MatchReasonFragmentConflict = "FRAGMENT_CONFLICT"
	MatchReasonNoMatch          = "NO_MATCH"
)

const MaxCallbackWindowMinutes = 1440

type MatchEvidence struct {
	Type    string `json:"type"`
	Field   string `json:"field"`
	Value   string `json:"value,omitempty"`
	Masked  string `json:"masked,omitempty"`
	Matched bool   `json:"matched"`
	Detail  string `json:"detail,omitempty"`
}

type MatchDecision struct {
	RequestID    string          `json:"requestId,omitempty"`
	Confidence   string          `json:"confidence"`
	Reason       string          `json:"reason"`
	Score        int             `json:"score"`
	Evidence     []MatchEvidence `json:"evidence"`
	Conflict     bool            `json:"conflict"`
	ConflictDesc string          `json:"conflictDescription,omitempty"`
}

type IdentityCandidate struct {
	RequestID string
	Status    string
	Fragments []IdentityFragment
}

type IdentityFragment struct {
	FirstName   string
	LastName    string
	DateOfBirth string
	Email       string
	Phone       string
	Channel     string
	EventID     string
}

func maskPhone(s string) string {
	d := normalizePhone(s)
	if len(d) < 4 {
		return "***"
	}
	return strings.Repeat("*", len(d)-4) + d[len(d)-4:]
}

func maskEmail(s string) string {
	at := strings.Index(s, "@")
	if at <= 1 {
		return "***"
	}
	return string(s[0]) + "***" + s[at:]
}

func evidenceFor(field, value string, matched bool, detail string) MatchEvidence {
	ev := MatchEvidence{Type: "identity", Field: field, Matched: matched, Detail: detail}
	if matched {
		ev.Value = value
	} else {
		switch field {
		case "phone":
			ev.Masked = maskPhone(value)
		case "email":
			ev.Masked = maskEmail(value)
		default:
			ev.Masked = value
		}
	}
	return ev
}

// MatchFragment compares an incoming fragment against a candidate's fragments
// deterministically. It returns the best (highest, then lowest requestID) match
// decision. Confidence rules:
//   - exact: firstName+lastName+dob all match on the same existing fragment
//   - high:  phone exact, or email exact (strong identifiers)
//   - medium: firstName+lastName match but dob missing on either side
//   - low:   only one of phone/email partially or name-only matches
//   - none:  no shared signal
//
// Conflicting values on a field that otherwise partially matches are recorded
// as conflict evidence without auto-merging into a second chain.
func MatchFragment(incoming IdentityFragment, candidates []IdentityCandidate) MatchDecision {
	best := MatchDecision{Confidence: ConfidenceNone, Reason: MatchReasonNoMatch}
	type scored struct {
		decision MatchDecision
	}
	var results []scored

	for _, cand := range candidates {
		dec := scoreCandidate(incoming, cand)
		results = append(results, scored{dec})
	}

	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i].decision, results[j].decision
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.RequestID < b.RequestID
	})

	if len(results) > 0 && results[0].decision.Score > 0 {
		best = results[0].decision
	}
	return best
}

func scoreCandidate(incoming IdentityFragment, cand IdentityCandidate) MatchDecision {
	dec := MatchDecision{RequestID: cand.RequestID, Confidence: ConfidenceNone}

	iFn, iLn := normalizeName(incoming.FirstName), normalizeName(incoming.LastName)
	iDob := strings.TrimSpace(incoming.DateOfBirth)
	iEmail := strings.ToLower(strings.TrimSpace(incoming.Email))
	iPhone := normalizePhone(incoming.Phone)

	var (
		bestScore         int
		bestReason        string
		bestConfidence    string
		evidence          []MatchEvidence
		hasExactIdentity  bool
		hasPhone          bool
		hasEmail          bool
		hasNameDobPartial bool
		conflictFields    []string
	)

	for _, f := range cand.Fragments {
		fFn, fLn := normalizeName(f.FirstName), normalizeName(f.LastName)
		fDob := strings.TrimSpace(f.DateOfBirth)
		fEmail := strings.ToLower(strings.TrimSpace(f.Email))
		fPhone := normalizePhone(f.Phone)

		var localScore int
		var localReason string
		var localConfidence = ConfidenceNone
		var localEv []MatchEvidence

		nameMatch := (iFn != "" && iFn == fFn) && (iLn != "" && iLn == fLn)
		dobMatch := iDob != "" && iDob == fDob
		phoneMatch := iPhone != "" && iPhone == fPhone
		emailMatch := iEmail != "" && iEmail == fEmail

		namePartial := (iFn != "" && iFn == fFn) || (iLn != "" && iLn == fLn)
		dobPresent := iDob != "" && fDob != ""
		dobConflict := dobPresent && iDob != fDob
		phoneConflict := iPhone != "" && fPhone != "" && iPhone != fPhone
		emailConflict := iEmail != "" && fEmail != "" && iEmail != fEmail

		if nameMatch && dobMatch {
			hasExactIdentity = true
			localScore += 100
			localReason = MatchReasonExactIdentity
			localConfidence = ConfidenceExact
			localEv = append(localEv,
				evidenceFor("firstName", incoming.FirstName, true, "exact"),
				evidenceFor("lastName", incoming.LastName, true, "exact"),
				evidenceFor("dateOfBirth", incoming.DateOfBirth, true, "exact"),
			)
		} else if nameMatch && (!dobPresent) {
			hasNameDobPartial = true
			localScore += 60
			if localConfidence == "" || confidenceRank(localConfidence) < confidenceRank(ConfidenceMedium) {
				localConfidence = ConfidenceMedium
			}
			if localReason == "" {
				localReason = MatchReasonNameDOBPartial
			}
			localEv = append(localEv, evidenceFor("name", incoming.FirstName+" "+incoming.LastName, true, "name match; DOB not available on one side"))
		} else if namePartial && dobMatch {
			hasNameDobPartial = true
			localScore += 55
			if confidenceRank(localConfidence) < confidenceRank(ConfidenceMedium) {
				localConfidence = ConfidenceMedium
			}
			if localReason == "" {
				localReason = MatchReasonNameDOBPartial
			}
			localEv = append(localEv, evidenceFor("dateOfBirth", incoming.DateOfBirth, true, "DOB match; partial name"))
		}

		if phoneMatch {
			hasPhone = true
			localScore += 80
			if confidenceRank(localConfidence) < confidenceRank(ConfidenceHigh) {
				localConfidence = ConfidenceHigh
			}
			if localReason == "" {
				localReason = MatchReasonPhoneMatch
			}
			localEv = append(localEv, evidenceFor("phone", incoming.Phone, true, "phone exact match"))
		}
		if emailMatch {
			hasEmail = true
			localScore += 80
			if confidenceRank(localConfidence) < confidenceRank(ConfidenceHigh) {
				localConfidence = ConfidenceHigh
			}
			if localReason == "" {
				localReason = MatchReasonEmailMatch
			}
			localEv = append(localEv, evidenceFor("email", incoming.Email, true, "email exact match"))
		}

		if nameMatch && dobConflict {
			conflictFields = append(conflictFields, "dateOfBirth")
			localEv = append(localEv, evidenceFor("dateOfBirth", incoming.DateOfBirth, false, "DOB differs from existing fragment; treated as conflict evidence, not auto-merge"))
		}
		if phoneConflict {
			conflictFields = append(conflictFields, "phone")
			localEv = append(localEv, evidenceFor("phone", incoming.Phone, false, "phone differs; retaining existing, recording conflict"))
		}
		if emailConflict {
			conflictFields = append(conflictFields, "email")
			localEv = append(localEv, evidenceFor("email", incoming.Email, false, "email differs; retaining existing, recording conflict"))
		}

		if localScore > bestScore {
			bestScore = localScore
			bestReason = localReason
			bestConfidence = localConfidence
			evidence = localEv
		}
	}

	if hasPhone || hasEmail {
		if bestConfidence == ConfidenceNone {
			bestConfidence = ConfidenceHigh
		}
		if bestReason == "" {
			if hasPhone {
				bestReason = MatchReasonPhoneMatch
			} else {
				bestReason = MatchReasonEmailMatch
			}
		}
	}
	if hasExactIdentity {
		bestConfidence = ConfidenceExact
		bestReason = MatchReasonExactIdentity
	}
	if (hasPhone || hasEmail) && (hasNameDobPartial || hasExactIdentity) {
		bestReason = MatchReasonCrossChannelLink
	}

	dec.Score = bestScore
	dec.Reason = bestReason
	dec.Confidence = bestConfidence
	dec.Evidence = evidence
	if len(conflictFields) > 0 {
		dec.Conflict = true
		dec.ConflictDesc = fmt.Sprintf("conflicting fields: %s", strings.Join(uniqueSorted(conflictFields), ", "))
	}
	if bestScore == 0 {
		dec.Confidence = ConfidenceNone
		dec.Reason = MatchReasonNoMatch
	}
	if dec.Confidence == ConfidenceNone {
		dec.Confidence = ConfidenceLow
	}
	return dec
}

func confidenceRank(c string) int {
	switch c {
	case ConfidenceExact:
		return 4
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

func uniqueSorted(in []string) []string {
	set := map[string]bool{}
	for _, v := range in {
		set[v] = true
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// AutoMerge returns true when the match is strong enough to automatically
// attach the incoming event to the existing canonical request.
func (d MatchDecision) AutoMerge() bool {
	return d.Confidence == ConfidenceExact || d.Confidence == ConfidenceHigh
}
