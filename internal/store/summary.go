package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

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
	// AcceptedCount is the number of distinct accepted events (chain links),
	// not the number of accepted attempts — duplicate replays re-report an
	// ACCEPTED status but do not add a link.
	accepted := len(req.Chain)
	sum := model.AuditSummary{
		CanonicalKey:  canonicalKey,
		Request:       req,
		Attempts:      attempts,
		Entries:       entries,
		AcceptedCount: accepted,
	}
	sum.Digest = digest(req, attempts, entries)
	return sum, true, nil
}

// digest computes a deterministic fingerprint over the normalized result and
// the ordered attempt/audit history, excluding wall-clock timestamps so that a
// faithful replay of the same event batch yields the same digest.
func digest(req model.CanonicalRequest, attempts []model.Attempt, entries []model.AuditEntry) string {
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
	}{req, av, ev}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
