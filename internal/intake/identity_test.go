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

// newTestServiceR2 mirrors newTestService but is named distinctly to make the
// Round 2 identity-resolution scenarios self-contained.
func newTestServiceR2(t *testing.T, down store.Downstream) *Service {
	t.Helper()
	c, err := contract.Parse([]byte(fixture))
	if err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	st, err := store.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	fixed := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	st.SetClock(func() time.Time { return fixed })
	t.Cleanup(func() { st.Close() })
	return New(c, st, down)
}

// TestCallbackBeforeWebBuildConverges is the core Round 2 case: a hotline
// callback carrying only corroborating identity fragments (phone/email) arrives
// BEFORE the web submission that first carries the correlation number APP-C.
// Both must land on one canonical request/chain — no second chain.
func TestCallbackBeforeWebBuildConverges(t *testing.T) {
	svc := newTestServiceR2(t, nil)
	ctx := context.Background()

	// 1. Hotline callback first, no correlation number yet, phone+email only.
	cb := raw(`{"eventId":"EV-H-CALL","channel":"HOTLINE","callRef":"C-900","callbackWindowMinutes":30,
		"identity":{"phone":"+1 (415) 555-0100","email":"Jamie@Example.com"}}`)
	r1, _ := svc.SubmitOne(ctx, cb)
	if r1.Status != model.StatusAccepted {
		t.Fatalf("callback not accepted: %s", r1.Status)
	}
	if r1.Match == nil || r1.Match.Reason != "NEW_FRAGMENTS" {
		t.Fatalf("expected NEW_FRAGMENTS, got %+v", r1.Match)
	}
	firstKey := r1.CanonicalKey

	// 2. Web submission later, carrying correlation APP-C and the same phone/email.
	web := raw(`{"eventId":"EV-W-BUILD","channel":"WEB","submissionId":"W-900","canonicalKey":"APP-C",
		"consent":["CONTACT_CALLBACK"],"identity":{"phone":"+1-415-555-0100","email":"jamie@example.com"}}`)
	r2, _ := svc.SubmitOne(ctx, web)
	if r2.Status != model.StatusAccepted {
		t.Fatalf("web not accepted: %s", r2.Status)
	}
	if r2.Match == nil || r2.Match.Reason != "CORROBORATED_MATCH" {
		t.Fatalf("expected CORROBORATED_MATCH binding to existing request, got %+v", r2.Match)
	}

	// Both keys must resolve to the SAME request with ONE chain of 2 events.
	byFirst, ok1, _ := svc.Store().GetCanonical(firstKey)
	byCorr, ok2, _ := svc.Store().GetCanonical("APP-C")
	if !ok1 || !ok2 {
		t.Fatalf("both keys must resolve: %v %v", ok1, ok2)
	}
	if byFirst.ID != byCorr.ID {
		t.Fatalf("callback and web formed different requests: %d vs %d", byFirst.ID, byCorr.ID)
	}
	if len(byCorr.Chain) != 2 {
		t.Fatalf("expected single chain of 2, got %d: %+v", len(byCorr.Chain), byCorr.Chain)
	}
}

// TestContactHiddenUntilConsent verifies raw contact info (phone/email + the
// callback window) is withheld until CONTACT_CALLBACK is confirmed, then
// exposed, and re-hidden (sticky) after revocation.
func TestContactHiddenUntilConsent(t *testing.T) {
	svc := newTestServiceR2(t, nil)
	ctx := context.Background()

	// Callback with contact fragments but NO consent yet.
	cb := raw(`{"eventId":"EV-H-1","channel":"HOTLINE","callRef":"C-1","canonicalKey":"APP-K","callbackWindowMinutes":0,
		"identity":{"phone":"+1 415 555 0111","email":"pat@example.com"}}`)
	if r, _ := svc.SubmitOne(ctx, cb); r.Status != model.StatusAccepted {
		t.Fatalf("callback not accepted: %s", r.Status)
	}
	req, _, _ := svc.Store().GetCanonical("APP-K")
	if req.Contact.Exposed {
		t.Fatalf("contact must NOT be exposed before consent")
	}
	if !req.Contact.HasPendingContact {
		t.Fatalf("should signal pending contact exists")
	}
	if len(req.Contact.Methods) != 0 || req.Contact.CallbackWindowMinutes != nil {
		t.Fatalf("raw contact leaked before consent: %+v", req.Contact)
	}
	// Non-identifying disposition may still be shown (0 min => IMMEDIATE).
	if req.Contact.CallbackDisposition != "IMMEDIATE" {
		t.Fatalf("disposition = %q, want IMMEDIATE", req.Contact.CallbackDisposition)
	}

	// Now consent is confirmed via a web event on the same request.
	grant := raw(`{"eventId":"EV-W-1","channel":"WEB","submissionId":"W-1","canonicalKey":"APP-K","consent":["CONTACT_CALLBACK"]}`)
	if r, _ := svc.SubmitOne(ctx, grant); r.Status != model.StatusAccepted {
		t.Fatalf("grant not accepted: %s", r.Status)
	}
	req2, _, _ := svc.Store().GetCanonical("APP-K")
	if !req2.Contact.Exposed || len(req2.Contact.Methods) == 0 || req2.Contact.CallbackWindowMinutes == nil {
		t.Fatalf("contact should be exposed after consent: %+v", req2.Contact)
	}

	// Revoke — contact must be hidden again and stay hidden.
	rev := raw(`{"eventId":"EV-R-1","revokes":"EV-W-1","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-K"}`)
	if r, _ := svc.SubmitOne(ctx, rev); r.Status != model.StatusAccepted {
		t.Fatalf("revoke not accepted: %s", r.Status)
	}
	req3, _, _ := svc.Store().GetCanonical("APP-K")
	if req3.Contact.Exposed || len(req3.Contact.Methods) != 0 || req3.Contact.CallbackWindowMinutes != nil {
		t.Fatalf("contact must be withheld after revocation: %+v", req3.Contact)
	}
	if !req3.Contact.HasPendingContact {
		t.Fatalf("pending flag should remain true (data still held, just gated)")
	}
}

// TestWindowDispositions checks zero/intraday/cross-day classification and that
// a negative window is rejected.
func TestWindowDispositions(t *testing.T) {
	svc := newTestServiceR2(t, nil)
	ctx := context.Background()
	cases := []struct {
		key, event string
		minutes    string
		wantDisp   string
		reject     bool
	}{
		{"W0", "EV-0", "0", "IMMEDIATE", false},
		{"W1", "EV-1", "600", "INTRADAY", false},
		{"W2", "EV-2", "1440", "CROSS_DAY", false},
		{"W3", "EV-3", "3000", "CROSS_DAY", false},
		{"W4", "EV-4", "-5", "", true},
	}
	for _, c := range cases {
		env := fmt.Sprintf(`{"eventId":%q,"channel":"HOTLINE","callRef":%q,"canonicalKey":%q,"callbackWindowMinutes":%s,"consent":["CONTACT_CALLBACK"]}`,
			c.event, "C-"+c.event, c.key, c.minutes)
		r, _ := svc.SubmitOne(ctx, raw(env))
		if c.reject {
			if r.Status != model.StatusRejected {
				t.Fatalf("%s: expected REJECTED for negative window, got %s", c.key, r.Status)
			}
			continue
		}
		if r.Status != model.StatusAccepted {
			t.Fatalf("%s: expected ACCEPTED, got %s (%+v)", c.key, r.Status, r.Errors)
		}
		req, _, _ := svc.Store().GetCanonical(c.key)
		if req.Contact.CallbackDisposition != c.wantDisp {
			t.Fatalf("%s: disposition = %q, want %q", c.key, req.Contact.CallbackDisposition, c.wantDisp)
		}
	}
}

// TestUniqueConflictDoesNotMerge ensures two different correlation numbers that
// happen to share a corroborating fragment are NOT merged; the shared fragment
// is recorded as conflict evidence instead.
func TestUniqueConflictDoesNotMerge(t *testing.T) {
	svc := newTestServiceR2(t, nil)
	ctx := context.Background()

	// Two distinct correlation numbers that share only a weak corroborating
	// fragment (postalCode) must NOT be merged into one request.
	a := raw(`{"eventId":"EV-A","channel":"WEB","submissionId":"W-A","canonicalKey":"APP-X","identity":{"postalCode":"94105"}}`)
	b := raw(`{"eventId":"EV-B","channel":"WEB","submissionId":"W-B","canonicalKey":"APP-Y","identity":{"postalCode":"94105"}}`)

	r1, _ := svc.SubmitOne(ctx, a)
	r2, _ := svc.SubmitOne(ctx, b)
	if r1.Status != model.StatusAccepted || r2.Status != model.StatusAccepted {
		t.Fatalf("both should be accepted: %s %s", r1.Status, r2.Status)
	}
	if r2.Match == nil || r2.Match.Reason != "UNIQUE_CONFLICT_NEW_REQUEST" {
		t.Fatalf("expected UNIQUE_CONFLICT_NEW_REQUEST, got %+v", r2.Match)
	}
	reqX, _, _ := svc.Store().GetCanonical("APP-X")
	reqY, _, _ := svc.Store().GetCanonical("APP-Y")
	if reqX.ID == reqY.ID {
		t.Fatalf("different correlation numbers must not merge")
	}
	if len(r2.Match.ConflictingOn) == 0 {
		t.Fatalf("shared postalCode should be recorded as conflict evidence: %+v", r2.Match)
	}
}

// TestConcurrentClaimSingleChainR2 fires the fragment-only callback and the
// correlation-bearing web event concurrently; exactly one chain must result.
func TestConcurrentClaimSingleChainR2(t *testing.T) {
	svc := newTestServiceR2(t, nil)
	ctx := context.Background()

	cb := raw(`{"eventId":"EV-CC-H","channel":"HOTLINE","callRef":"C-CC","identity":{"phone":"+1 202 555 0000"}}`)
	web := raw(`{"eventId":"EV-CC-W","channel":"WEB","submissionId":"W-CC","canonicalKey":"APP-CC","identity":{"phone":"+1 202 555 0000"}}`)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); svc.SubmitOne(ctx, cb) }()
	go func() { defer wg.Done(); svc.SubmitOne(ctx, web) }()
	wg.Wait()

	// Count distinct canonical requests that own either event.
	req, ok, err := svc.Store().GetCanonical("APP-CC")
	if err != nil || !ok {
		t.Fatalf("APP-CC not found: %v", err)
	}
	// The two events share a phone fragment, so both must be on one chain.
	if len(req.Chain) != 2 {
		t.Fatalf("concurrent fragment claim must form one chain of 2, got %d: %+v", len(req.Chain), req.Chain)
	}
}

// TestRetryRecoveryNoSecondChain verifies a transient adapter failure on the
// fragment-only callback, followed by a successful retry and the web build,
// still yields exactly one chain.
func TestRetryRecoveryNoSecondChain(t *testing.T) {
	var mu sync.Mutex
	failOnce := map[string]bool{"EV-RC-H": true}
	down := func(ev *model.CanonicalEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if failOnce[ev.EventID] {
			failOnce[ev.EventID] = false
			return &store.TransientError{Err: fmt.Errorf("adapter down")}
		}
		return nil
	}
	svc := newTestServiceR2(t, down)
	ctx := context.Background()

	cb := raw(`{"eventId":"EV-RC-H","channel":"HOTLINE","callRef":"C-RC","identity":{"email":"rc@example.com"}}`)
	// First attempt fails transiently: nothing persisted, so no request/fragment yet.
	r1, _ := svc.SubmitOne(ctx, cb)
	if r1.Status != model.StatusFailed || !r1.Retriable {
		t.Fatalf("expected retriable FAILED, got %s", r1.Status)
	}

	// Web build arrives (also carrying the email) and creates the request.
	web := raw(`{"eventId":"EV-RC-W","channel":"WEB","submissionId":"W-RC","canonicalKey":"APP-RC","identity":{"email":"rc@example.com"}}`)
	if r, _ := svc.SubmitOne(ctx, web); r.Status != model.StatusAccepted {
		t.Fatalf("web not accepted: %s", r.Status)
	}

	// Retry the callback: it must converge onto the existing request, not fork.
	r2, _ := svc.SubmitOne(ctx, cb)
	if r2.Status != model.StatusAccepted {
		t.Fatalf("retry should succeed: %s", r2.Status)
	}
	req, _, _ := svc.Store().GetCanonical("APP-RC")
	if len(req.Chain) != 2 {
		t.Fatalf("failure recovery must not fork the chain, got %d: %+v", len(req.Chain), req.Chain)
	}
}

// TestOutOfOrderReplayStableEvidence submits the callback-first batch twice into
// separate stores and confirms the audit digest (including match evidence) is
// identical — deterministic match reasons and confidence.
func TestOutOfOrderReplayStableEvidence(t *testing.T) {
	batch := []json.RawMessage{
		raw(`{"eventId":"EV-H-CALL","channel":"HOTLINE","callRef":"C-900","callbackWindowMinutes":30,"identity":{"phone":"+1 (415) 555-0100"}}`),
		raw(`{"eventId":"EV-W-BUILD","channel":"WEB","submissionId":"W-900","canonicalKey":"APP-C","consent":["CONTACT_CALLBACK"],"identity":{"phone":"+1-415-555-0100"}}`),
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
		sum, ok, err := st.Summary("APP-C")
		if err != nil || !ok {
			t.Fatalf("summary: ok=%v err=%v", ok, err)
		}
		if len(sum.MatchEvidence) != 2 {
			t.Fatalf("expected 2 evidence rows, got %d", len(sum.MatchEvidence))
		}
		return sum.Digest
	}
	if d1, d2 := run("evA"), run("evB"); d1 != d2 {
		t.Fatalf("evidence-inclusive digest not stable: %s vs %s", d1, d2)
	}
}
