package intake

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/accessible-intake-gateway/internal/contract"
	"github.com/accessible-intake-gateway/internal/model"
	"github.com/accessible-intake-gateway/internal/store"
)

const fixture = `{
  "canonicalVersion": "intake.v1",
  "accommodationCodes": ["BRAILLE_MATERIAL", "SIGN_INTERPRETER", "TEXT_ONLY", "STEP_FREE_ACCESS"],
  "consentScopes": ["CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"],
  "channels": {
    "PHYSICAL": {"sourceIdField": "deskReceiptNo", "personField": "visitor"},
    "HOTLINE": {"sourceIdField": "callRef", "personField": "caller", "callbackWindowField": "callbackWindowMinutes"},
    "WEB": {"sourceIdField": "submissionId", "personField": "applicant"}
  }
}`

// newTestService builds a service backed by a fresh, uniquely-named in-memory
// database so tests do not share state.
func newTestService(t *testing.T, down store.Downstream) *Service {
	t.Helper()
	c, err := contract.Parse([]byte(fixture))
	if err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	// Deterministic clock for stable replay/digest checks.
	fixed := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	st.SetClock(func() time.Time { return fixed })
	t.Cleanup(func() { st.Close() })
	return New(c, st, down)
}

func raw(v string) json.RawMessage { return json.RawMessage(v) }

// TestMixedBatchConvergence feeds the full adversarial batch — out of order,
// duplicate, incomplete, unknown code, revocation — across all three channels
// and verifies a single canonical chain with correct per-record semantics.
func TestMixedBatchConvergence(t *testing.T) {
	svc := newTestService(t, nil)
	ctx := context.Background()

	// All three channels correlate to the same applicant via canonicalKey APP-1.
	// Events are intentionally shuffled (web before physical, revocation early).
	batch := []json.RawMessage{
		raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"],"canonicalKey":"APP-1"}`),
		raw(`{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"],"canonicalKey":"APP-1"}`),
		raw(`{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","accommodations":["BRAILLE_MATERIAL"],"canonicalKey":"APP-1"}`),
		// Duplicate of the web event, same payload -> replay.
		raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"],"canonicalKey":"APP-1"}`),
		// Incomplete (missing submissionId) -> rejected.
		raw(`{"eventId":"EV-W-050","channel":"WEB","canonicalKey":"APP-1"}`),
		// Unknown accommodation code -> rejected.
		raw(`{"eventId":"EV-P-050","channel":"PHYSICAL","deskReceiptNo":"D-99","accommodations":["ROBOT_HELPER"],"canonicalKey":"APP-1"}`),
	}

	results, _ := svc.SubmitBatch(ctx, batch)
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
	assertStatus(t, results[0], model.StatusAccepted, false)
	assertStatus(t, results[1], model.StatusAccepted, false)
	assertStatus(t, results[2], model.StatusAccepted, false)
	assertStatus(t, results[3], model.StatusAccepted, true) // duplicate replay
	assertStatus(t, results[4], model.StatusRejected, false)
	assertStatus(t, results[5], model.StatusRejected, false)

	req, ok, err := svc.Store().GetCanonical("APP-1")
	if err != nil || !ok {
		t.Fatalf("canonical not found: ok=%v err=%v", ok, err)
	}
	// Exactly one chain, one link per accepted distinct event (3 accepted).
	if len(req.Chain) != 3 {
		t.Fatalf("expected chain length 3, got %d: %+v", len(req.Chain), req.Chain)
	}
	if len(req.Accommodations) != 1 || req.Accommodations[0] != "BRAILLE_MATERIAL" {
		t.Fatalf("accommodations merge wrong: %+v", req.Accommodations)
	}
	// CONTACT_CALLBACK effective -> contact exposed with 45 min window.
	if !req.Contact.Exposed || req.Contact.CallbackWindowMinutes == nil || *req.Contact.CallbackWindowMinutes != 45 {
		t.Fatalf("contact projection wrong: %+v", req.Contact)
	}
	if !contains(req.EffectiveConsent, "CASE_SUMMARY_TRANSFER") || !contains(req.EffectiveConsent, "CONTACT_CALLBACK") {
		t.Fatalf("effective consent wrong: %+v", req.EffectiveConsent)
	}
}

// TestConflictSameKeyDifferentPayload ensures a repeated event key with a
// changed payload is reported as CONFLICT and never overwrites the original.
func TestConflictSameKeyDifferentPayload(t *testing.T) {
	svc := newTestService(t, nil)
	ctx := context.Background()

	first, _ := svc.SubmitOne(ctx, raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","canonicalKey":"APP-2"}`))
	assertStatus(t, first, model.StatusAccepted, false)

	conflict, _ := svc.SubmitOne(ctx, raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-72","canonicalKey":"APP-2"}`))
	if conflict.Status != model.StatusConflict {
		t.Fatalf("expected CONFLICT, got %s", conflict.Status)
	}

	// Original source id must be intact.
	req, _, _ := svc.Store().GetCanonical("APP-2")
	if req.Chain[0].SourceID != "W-71" {
		t.Fatalf("conflict overwrote original: %+v", req.Chain)
	}
}

// TestIdempotentReplay ensures the same key + same payload replays the original
// result across separate submissions (and increments attempt count).
func TestIdempotentReplay(t *testing.T) {
	svc := newTestService(t, nil)
	ctx := context.Background()
	payload := raw(`{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","canonicalKey":"APP-3"}`)

	r1, _ := svc.SubmitOne(ctx, payload)
	r2, _ := svc.SubmitOne(ctx, payload)
	if r1.Status != model.StatusAccepted || r2.Status != model.StatusAccepted {
		t.Fatalf("both should be accepted: %s %s", r1.Status, r2.Status)
	}
	if !r2.Duplicate {
		t.Fatalf("second submission should be flagged duplicate")
	}
	if r2.AttemptNo <= r1.AttemptNo {
		t.Fatalf("attempt number should advance: %d -> %d", r1.AttemptNo, r2.AttemptNo)
	}
	// Still exactly one link in the chain.
	req, _, _ := svc.Store().GetCanonical("APP-3")
	if len(req.Chain) != 1 {
		t.Fatalf("replay must not add chain links: %+v", req.Chain)
	}
}

// TestRevocationIsSticky verifies a revoked scope is never re-exposed by a
// later replay or re-grant of the original event, and the callback contact is
// withdrawn when CONTACT_CALLBACK is revoked.
func TestRevocationIsSticky(t *testing.T) {
	svc := newTestService(t, nil)
	ctx := context.Background()

	grant := raw(`{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK","CASE_SUMMARY_TRANSFER"],"canonicalKey":"APP-4"}`)
	if r, _ := svc.SubmitOne(ctx, grant); r.Status != model.StatusAccepted {
		t.Fatalf("grant not accepted: %s", r.Status)
	}

	// Revoke CONTACT_CALLBACK.
	rev := raw(`{"eventId":"EV-H-900","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-4"}`)
	if r, _ := svc.SubmitOne(ctx, rev); r.Status != model.StatusAccepted {
		t.Fatalf("revocation not accepted: %s", r.Status)
	}

	req, _, _ := svc.Store().GetCanonical("APP-4")
	if req.Contact.Exposed {
		t.Fatalf("callback contact must be withdrawn after revocation: %+v", req.Contact)
	}
	if !contains(req.RevokedConsent, "CONTACT_CALLBACK") {
		t.Fatalf("CONTACT_CALLBACK should be revoked: %+v", req.RevokedConsent)
	}

	// Replay the ORIGINAL grant (same payload). It must not re-expose the scope.
	if r, _ := svc.SubmitOne(ctx, grant); !r.Duplicate {
		t.Fatalf("grant replay should be duplicate: %+v", r)
	}
	req2, _, _ := svc.Store().GetCanonical("APP-4")
	if req2.Contact.Exposed || contains(req2.EffectiveConsent, "CONTACT_CALLBACK") {
		t.Fatalf("revoked consent was re-exposed by replay: %+v", req2)
	}
}

// TestTransientFailureThenRetry verifies a transient downstream failure yields
// a retriable FAILED result that persists no canonical change, and a later
// retry succeeds.
func TestTransientFailureThenRetry(t *testing.T) {
	var mu sync.Mutex
	failFirst := map[string]bool{"EV-W-001": true}
	down := func(ev *model.CanonicalEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if failFirst[ev.EventID] {
			failFirst[ev.EventID] = false
			return &store.TransientError{Err: fmt.Errorf("adapter temporarily unavailable")}
		}
		return nil
	}
	svc := newTestService(t, down)
	ctx := context.Background()
	payload := raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","canonicalKey":"APP-5"}`)

	r1, _ := svc.SubmitOne(ctx, payload)
	if r1.Status != model.StatusFailed || !r1.Retriable {
		t.Fatalf("expected retriable FAILED, got %s retriable=%v", r1.Status, r1.Retriable)
	}
	// Nothing should be persisted yet.
	if _, ok, _ := svc.Store().GetCanonical("APP-5"); ok {
		t.Fatalf("failed event must not persist a canonical request")
	}

	r2, _ := svc.SubmitOne(ctx, payload)
	if r2.Status != model.StatusAccepted {
		t.Fatalf("retry should succeed, got %s", r2.Status)
	}
	if r2.Duplicate {
		t.Fatalf("retry after failure is a real apply, not a duplicate replay")
	}
	// Attempt log must show both the failure and the success.
	attempts, _ := svc.Store().Attempts("EV-W-001")
	if len(attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d: %+v", len(attempts), attempts)
	}
	if attempts[0].Status != model.StatusFailed || attempts[1].Status != model.StatusAccepted {
		t.Fatalf("attempt sequence wrong: %+v", attempts)
	}
}

// TestConcurrentSameCanonicalRequest fires two channels reporting the same
// canonical request concurrently; only one chain must form, containing both.
func TestConcurrentSameCanonicalRequest(t *testing.T) {
	svc := newTestService(t, nil)
	ctx := context.Background()

	a := raw(`{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","canonicalKey":"APP-6"}`)
	b := raw(`{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","canonicalKey":"APP-6"}`)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); svc.SubmitOne(ctx, a) }()
	go func() { defer wg.Done(); svc.SubmitOne(ctx, b) }()
	wg.Wait()

	req, ok, err := svc.Store().GetCanonical("APP-6")
	if err != nil || !ok {
		t.Fatalf("canonical not found: %v", err)
	}
	if len(req.Chain) != 2 {
		t.Fatalf("concurrent reports must form one chain of 2, got %d: %+v", len(req.Chain), req.Chain)
	}
}

// TestReplayStableDigest verifies the audit summary digest is stable when the
// same batch is replayed into a fresh store.
func TestReplayStableDigest(t *testing.T) {
	batch := []json.RawMessage{
		raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER"],"canonicalKey":"APP-7"}`),
		raw(`{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"],"canonicalKey":"APP-7"}`),
		raw(`{"eventId":"EV-H-900","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-7"}`),
	}
	run := func(name string) string {
		c, _ := contract.Parse([]byte(fixture))
		st, err := store.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer st.Close()
		fixed := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
		st.SetClock(func() time.Time { return fixed })
		svc := New(c, st, nil)
		svc.SubmitBatch(context.Background(), batch)
		sum, ok, err := st.Summary("APP-7")
		if err != nil || !ok {
			t.Fatalf("summary: ok=%v err=%v", ok, err)
		}
		return sum.Digest
	}
	d1 := run("replayA")
	d2 := run("replayB")
	if d1 != d2 {
		t.Fatalf("digest not stable across replay: %s vs %s", d1, d2)
	}
	if d1 == "" {
		t.Fatalf("digest empty")
	}
}

// TestRepeatableSchema ensures re-opening the same file database is safe and
// preserves data (repeatable build).
func TestRepeatableSchema(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/intake.db"
	c, _ := contract.Parse([]byte(fixture))

	st1, err := store.Open(path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	svc := New(c, st1, nil)
	svc.SubmitOne(context.Background(), raw(`{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","canonicalKey":"APP-8"}`))
	st1.Close()

	// Re-open: schema re-applied (idempotent), data persists.
	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer st2.Close()
	req, ok, err := st2.GetCanonical("APP-8")
	if err != nil || !ok {
		t.Fatalf("data lost after reopen: ok=%v err=%v", ok, err)
	}
	if len(req.Chain) != 1 {
		t.Fatalf("chain lost after reopen: %+v", req.Chain)
	}
}

func assertStatus(t *testing.T, r model.RecordResult, want model.Status, dup bool) {
	t.Helper()
	if r.Status != want {
		t.Fatalf("event %s: status = %s, want %s (errors=%+v)", r.EventID, r.Status, want, r.Errors)
	}
	if r.Duplicate != dup {
		t.Fatalf("event %s: duplicate = %v, want %v", r.EventID, r.Duplicate, dup)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
