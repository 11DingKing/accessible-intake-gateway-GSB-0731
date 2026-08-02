package gateway

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Match decisions recorded on every accepted INTAKE result.
const (
	MatchExact      = "EXACT"      // exact person-key hit (or concurrent claim adopted)
	MatchMerged     = "MERGED"     // identity evidence merged the event into an existing request
	MatchCandidate  = "CANDIDATE"  // partial overlap, insufficient or blocked: new standalone request
	MatchNew        = "NEW"        // no identity overlap at all
	MatchStandalone = "STANDALONE" // no person identity supplied
)

// Candidate outcomes while evaluating one existing request.
const (
	outcomeMerge            = "MERGE"
	outcomeNameOnly         = "NAME_ONLY"
	outcomeConflictRejected = "CONFLICT_REJECTED"
)

// Identity fragment kinds.
const (
	identityName     = "name"
	identityPhone    = "phone"
	identityEmail    = "email"
	identityIDNumber = "idNumber"
)

// Deterministic confidence scoring for identity evidence.
const (
	scoreIDNumberMatch    = 100
	scorePhoneMatch       = 50
	scoreEmailMatch       = 50
	scoreNameMatch        = 10
	penaltyStrongConflict = -100
	penaltyIDConflict     = -1000
)

// fragment is one normalized identity fragment. Raw values stay inside the
// store layer; match evidence published in results carries only hashes.
type fragment struct {
	Kind  string
	Value string
}

// personFragments extracts normalized identity fragments from a person.
func personFragments(p *Person) []fragment {
	var out []fragment
	if n := normalizeName(p.Name); n != "" {
		out = append(out, fragment{identityName, n})
	}
	if ph := digitsOnly(p.Phone); ph != "" {
		out = append(out, fragment{identityPhone, ph})
	}
	if e := strings.ToLower(strings.TrimSpace(p.Email)); e != "" {
		out = append(out, fragment{identityEmail, e})
	}
	if id := strings.ToUpper(strings.TrimSpace(p.IDNumber)); id != "" {
		out = append(out, fragment{identityIDNumber, id})
	}
	return out
}

// fragmentHash is the stable, non-raw evidence handle for one fragment.
func fragmentHash(kind, value string) string {
	sum := sha256.Sum256([]byte(kind + "|" + value))
	return hex.EncodeToString(sum[:])[:16]
}

// FragmentEvidence is one matched or conflicted fragment, by kind and hash
// only — never the raw value, so evidence is safe to expose before consent.
// Redacted marks a match against non-reversible erased-fragment evidence.
type FragmentEvidence struct {
	Kind         string `json:"kind"`
	FragmentHash string `json:"fragmentHash"`
	Redacted     bool   `json:"redacted,omitempty"`
}

// CandidateEvidence is the per-candidate evaluation trail.
type CandidateEvidence struct {
	RequestID  string             `json:"requestId"`
	Outcome    string             `json:"outcome"` // MERGE | NAME_ONLY | CONFLICT_REJECTED
	Score      int                `json:"score"`
	Matched    []FragmentEvidence `json:"matched,omitempty"`
	Conflicted []FragmentEvidence `json:"conflicted,omitempty"`
}

// MatchEvidence is the deterministic rationale recorded on an accepted
// INTAKE result: why the event landed on this request, with what confidence.
type MatchEvidence struct {
	Decision   string              `json:"decision"` // EXACT | MERGED | CANDIDATE | NEW | STANDALONE
	RequestID  string              `json:"requestId"`
	Score      int                 `json:"score"`
	Rationale  []string            `json:"rationale"`
	Considered []CandidateEvidence `json:"considered,omitempty"`
}

// evidenceSet indexes erased-fragment hashes by kind for one request.
type evidenceSet map[string]map[string]bool

func buildEvidenceSet(rows []ErasureRow) evidenceSet {
	set := evidenceSet{}
	for _, r := range rows {
		if set[r.Kind] == nil {
			set[r.Kind] = map[string]bool{}
		}
		set[r.Kind][r.FragmentHash] = true
	}
	return set
}

// evaluateCandidate compares the event's fragments with one request's live
// fragments and erased evidence, returning matched/conflicted evidence
// (sorted by kind) and the confidence score. A fragment matching only
// erased evidence still scores as a match — the hash proves the association
// without retaining the raw value — but erased fragments can never conflict.
func evaluateCandidate(eventFrags, requestFrags []fragment, erased evidenceSet) (matched, conflicted []FragmentEvidence, score int) {
	byKind := map[string][]string{}
	for _, f := range requestFrags {
		byKind[f.Kind] = append(byKind[f.Kind], f.Value)
	}
	for _, f := range eventFrags {
		hash := fragmentHash(f.Kind, f.Value)
		values := byKind[f.Kind]
		if contains(values, f.Value) {
			matched = append(matched, FragmentEvidence{Kind: f.Kind, FragmentHash: hash})
			score += matchScore(f.Kind)
			continue
		}
		if erased[f.Kind][hash] {
			matched = append(matched, FragmentEvidence{Kind: f.Kind, FragmentHash: hash, Redacted: true})
			score += matchScore(f.Kind)
			continue
		}
		if len(values) == 0 {
			continue
		}
		conflicted = append(conflicted, FragmentEvidence{Kind: f.Kind, FragmentHash: hash})
		switch f.Kind {
		case identityIDNumber:
			score += penaltyIDConflict
		case identityPhone, identityEmail:
			score += penaltyStrongConflict
		}
		// name differences carry no penalty: names are naturally fuzzy.
	}
	sortEvidence(matched)
	sortEvidence(conflicted)
	return matched, conflicted, score
}

func matchScore(kind string) int {
	switch kind {
	case identityIDNumber:
		return scoreIDNumberMatch
	case identityPhone:
		return scorePhoneMatch
	case identityEmail:
		return scoreEmailMatch
	default:
		return scoreNameMatch
	}
}

func sortEvidence(evs []FragmentEvidence) {
	sort.Slice(evs, func(i, j int) bool {
		if evs[i].Kind != evs[j].Kind {
			return evs[i].Kind < evs[j].Kind
		}
		return evs[i].FragmentHash < evs[j].FragmentHash
	})
}

// candidateOutcome applies the deterministic merge rules:
// any strong-identifier conflict blocks merging; otherwise any
// strong-identifier match merges (including redacted-evidence matches);
// a name-only overlap stays a candidate.
func candidateOutcome(matched, conflicted []FragmentEvidence) string {
	for _, f := range conflicted {
		if f.Kind == identityIDNumber || f.Kind == identityPhone || f.Kind == identityEmail {
			return outcomeConflictRejected
		}
	}
	for _, f := range matched {
		if f.Kind != identityName {
			return outcomeMerge
		}
	}
	return outcomeNameOnly
}

// kindsOf renders evidence kinds for rationale; redacted matches are marked.
func kindsOf(evs []FragmentEvidence) string {
	kinds := make([]string, 0, len(evs))
	for _, e := range evs {
		kind := e.Kind
		if e.Redacted {
			kind += "(redacted)"
		}
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return strings.Join(kinds, ",")
}

// resolveRequestTx decides which canonical request an INTAKE event attaches
// to. It runs inside the record's write transaction; because writes are
// serialized, concurrent channel claims evaluate the same committed state
// and can never fork a second chain for the same identity.
func (s *Service) resolveRequestTx(ctx context.Context, tx *sql.Tx, env *Envelope) (*RequestRow, *MatchEvidence, error) {
	now := s.stamp()
	create := func(personKey string) (*RequestRow, bool, error) {
		personJSON := ""
		if env.Person != nil {
			personJSON = string(mustMarshal(env.Person))
		}
		candidate := &RequestRow{
			RequestID:  newRequestID(),
			PersonKey:  personKey,
			PersonJSON: personJSON,
			StateJSON:  string(mustMarshal(newRequestState(s.reg.CanonicalVersion))),
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		return InsertRequestIfAbsentTx(ctx, tx, candidate)
	}

	if env.Person == nil {
		req, _, err := create("event:" + env.EventID)
		if err != nil {
			return nil, nil, err
		}
		return req, &MatchEvidence{
			Decision:  MatchStandalone,
			RequestID: req.RequestID,
			Rationale: []string{"no person identity supplied; standalone request keyed by eventId"},
		}, nil
	}

	key := personMergeKey(env)
	if existing, err := GetRequestByPersonKeyTx(ctx, tx, key); err != nil {
		return nil, nil, err
	} else if existing != nil {
		frags := personFragments(env.Person)
		_, _, score := evaluateCandidate(frags, frags, nil)
		return existing, &MatchEvidence{
			Decision:  MatchExact,
			RequestID: existing.RequestID,
			Score:     score,
			Rationale: []string{"exact identity key match"},
		}, nil
	}

	frags := personFragments(env.Person)
	candidates, err := FindCandidateRequestsTx(ctx, tx, frags)
	if err != nil {
		return nil, nil, err
	}
	var considered []CandidateEvidence
	var mergeTarget *RequestRow
	var mergeEvidence CandidateEvidence
	for _, c := range candidates {
		requestFrags, err := ListIdentitiesTx(ctx, tx, c.RequestID)
		if err != nil {
			return nil, nil, err
		}
		erasedRows, err := ListErasedEvidenceTx(ctx, tx, c.RequestID)
		if err != nil {
			return nil, nil, err
		}
		matched, conflicted, score := evaluateCandidate(frags, requestFrags, buildEvidenceSet(erasedRows))
		outcome := candidateOutcome(matched, conflicted)
		ev := CandidateEvidence{
			RequestID:  c.RequestID,
			Outcome:    outcome,
			Score:      score,
			Matched:    matched,
			Conflicted: conflicted,
		}
		considered = append(considered, ev)
		// Candidates arrive oldest-first: the first MERGE is the oldest.
		if outcome == outcomeMerge && mergeTarget == nil {
			mergeTarget = c
			mergeEvidence = ev
		}
	}

	if mergeTarget != nil {
		rationale := []string{fmt.Sprintf("merged into %s: %s matched with no strong-identifier conflict",
			mergeTarget.RequestID, kindsOf(mergeEvidence.Matched))}
		if len(considered) > 1 {
			rationale = append(rationale, fmt.Sprintf("%d candidates considered; oldest merge candidate selected", len(considered)))
		}
		return mergeTarget, &MatchEvidence{
			Decision:   MatchMerged,
			RequestID:  mergeTarget.RequestID,
			Score:      mergeEvidence.Score,
			Rationale:  rationale,
			Considered: considered,
		}, nil
	}

	req, created, err := create(key)
	if err != nil {
		return nil, nil, err
	}
	if !created {
		return req, &MatchEvidence{
			Decision:  MatchExact,
			RequestID: req.RequestID,
			Rationale: []string{"concurrent exact-key claim adopted the existing request"},
		}, nil
	}
	if len(considered) > 0 {
		best := 0
		var rationale []string
		for _, ev := range considered {
			if ev.Score > best {
				best = ev.Score
			}
			switch ev.Outcome {
			case outcomeNameOnly:
				rationale = append(rationale, fmt.Sprintf("candidate %s: name matched but no shared strong identifier", ev.RequestID))
			case outcomeConflictRejected:
				rationale = append(rationale, fmt.Sprintf("candidate %s rejected: %s conflict", ev.RequestID, kindsOf(ev.Conflicted)))
			}
		}
		rationale = append(rationale, "no mergeable candidate; new standalone request")
		return req, &MatchEvidence{
			Decision:   MatchCandidate,
			RequestID:  req.RequestID,
			Score:      best,
			Rationale:  rationale,
			Considered: considered,
		}, nil
	}
	return req, &MatchEvidence{
		Decision:  MatchNew,
		RequestID: req.RequestID,
		Rationale: []string{"no identity overlap; new request"},
	}, nil
}

// accumulatePersonTx folds newly seen person fields into the request's
// stored person record (first non-empty value wins per field) and registers
// any new identity fragments, so later candidates can match on them.
//
// allowContact gates contact fields (phone/email): once a request's contact
// data has been erased, a later event may only re-register contact details
// when it carries a fresh CONTACT_CALLBACK grant. Identity fields that are
// not contact channels (name, idNumber) always accumulate.
func accumulatePersonTx(ctx context.Context, tx *sql.Tx, req *RequestRow, incoming *Person, allowContact bool) error {
	if incoming == nil {
		return nil
	}
	frags := personFragments(incoming)
	if !allowContact {
		kept := frags[:0]
		for _, f := range frags {
			if f.Kind != identityPhone && f.Kind != identityEmail {
				kept = append(kept, f)
			}
		}
		frags = kept
	}
	if req.PersonJSON != "" {
		var stored Person
		if err := json.Unmarshal([]byte(req.PersonJSON), &stored); err != nil {
			return fmt.Errorf("stored person unreadable: %w", err)
		}
		changed := false
		if allowContact && stored.Phone == "" && incoming.Phone != "" {
			stored.Phone = incoming.Phone
			changed = true
		}
		if allowContact && stored.Email == "" && incoming.Email != "" {
			stored.Email = incoming.Email
			changed = true
		}
		if stored.IDNumber == "" && incoming.IDNumber != "" {
			stored.IDNumber = incoming.IDNumber
			changed = true
		}
		if changed {
			if err := UpdateRequestPersonTx(ctx, tx, req.RequestID, string(mustMarshal(&stored))); err != nil {
				return err
			}
		}
	}
	return AddIdentitiesTx(ctx, tx, req.RequestID, frags)
}
