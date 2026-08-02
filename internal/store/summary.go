package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/accessible-intake-gateway/internal/model"
)

func jsonUnmarshal(s string, v interface{}) error {
	return json.Unmarshal([]byte(s), v)
}

// Summary assembles the minimal, replay-stable audit summary for a canonical
// request: the projected request, every attempt, the audit trail, and a digest
// that is stable across identical replays (it omits volatile timestamps).
func (s *Store) Summary(canonicalKey string) (model.AuditSummary, bool, error) {
	req, ok, err := s.GetCanonical(canonicalKey)
	if err != nil || !ok {
		return model.AuditSummary{}, ok, err
	}
	attempts, err := s.AttemptsForKey(canonicalKey)
	if err != nil {
		return model.AuditSummary{}, false, err
	}
	entries, err := s.AuditTrail(canonicalKey)
	if err != nil {
		return model.AuditSummary{}, false, err
	}
	evidence, err := s.MatchEvidence(req.CanonicalKey)
	if err != nil {
		return model.AuditSummary{}, false, err
	}
	// AcceptedCount is the number of distinct accepted events (chain links),
	// not the number of accepted attempts — duplicate replays re-report an
	// ACCEPTED status but do not add a link.
	accepted := len(req.Chain)
	sum := model.AuditSummary{
		CanonicalKey:  req.CanonicalKey,
		Request:       req,
		Attempts:      attempts,
		Entries:       entries,
		MatchEvidence: evidence,
		AcceptedCount: accepted,
	}
	sum.Digest = digest(req, attempts, entries, evidence)
	return sum, true, nil
}

// MatchEvidence returns the deterministic match decisions for every event bound
// to the canonical request (resolved by canonical key or alias), ordered by the
// event's chain position for stable replay.
func (s *Store) MatchEvidence(canonicalKey string) ([]model.MatchDecision, error) {
	rows, err := s.db.Query(`SELECT me.canonical_key, me.reason, me.confidence, me.score, COALESCE(me.matched_on,''), COALESCE(me.conflicting_on,''), me.new_request
		FROM match_evidence me
		JOIN event_link el ON el.event_id = me.event_id
		WHERE el.request_id = (
			SELECT id FROM canonical_request WHERE canonical_key = ?
			UNION SELECT request_id FROM request_alias WHERE alias_key = ?
		)
		ORDER BY el.seq`, canonicalKey, canonicalKey)
	if err != nil {
		return nil, fmt.Errorf("load match evidence: %w", err)
	}
	defer rows.Close()
	var out []model.MatchDecision
	for rows.Next() {
		var d model.MatchDecision
		var matched, conflicting string
		var newReq int
		if err := rows.Scan(&d.CanonicalKey, &d.Reason, &d.Confidence, &d.Score, &matched, &conflicting, &newReq); err != nil {
			return nil, err
		}
		d.MatchedOn = splitList(matched)
		d.ConflictingOn = splitList(conflicting)
		d.NewRequest = newReq == 1
		out = append(out, d)
	}
	return out, nil
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// digest computes a deterministic fingerprint over the normalized result and
// the ordered attempt/audit history, excluding wall-clock timestamps so that a
// faithful replay of the same event batch yields the same digest.
func digest(req model.CanonicalRequest, attempts []model.Attempt, entries []model.AuditEntry, evidence []model.MatchDecision) string {
	type attemptView struct {
		EventID     string
		AttemptNo   int
		Status      model.Status
		PayloadHash string
		Errors      []model.RecordError
	}
	type auditView struct {
		EventID string
		Action  string
		Detail  string
	}
	av := make([]attemptView, 0, len(attempts))
	for _, a := range attempts {
		av = append(av, attemptView{a.EventID, a.AttemptNo, a.Status, a.PayloadHash, a.Errors})
	}
	ev := make([]auditView, 0, len(entries))
	for _, e := range entries {
		ev = append(ev, auditView{e.EventID, e.Action, e.Detail})
	}
	// Zero volatile identifiers/sequences that do not affect the normalized state.
	req.ID = 0
	req.CreatedSeq = 0
	req.UpdatedSeq = 0
	for i := range req.Chain {
		req.Chain[i].Seq = 0
	}
	payload := struct {
		Request  model.CanonicalRequest
		Attempts []attemptView
		Audit    []auditView
		Evidence []model.MatchDecision
	}{req, av, ev, evidence}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
