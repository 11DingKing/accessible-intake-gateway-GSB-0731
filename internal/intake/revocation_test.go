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

// newTestServiceR3 builds a service on a fresh named in-memory DB with a fixed
// clock, for the Round 3 revocation/erasure scenarios.
func newTestServiceR3(t *testing.T, name string, down store.Downstream) *Service {
	t.Helper()
	c, err := contract.Parse([]byte(fixture))
	if err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	st, err := store.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	fixed := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	st.SetClock(func() time.Time { return fixed })
	t.Cleanup(func() { st.Close() })
	return New(c, st, down)
}

// The Round 3 fixture models the contract's revocation example: EV-W-001 grants
// CONTACT_CALLBACK + CASE_SUMMARY_TRANSFER on APP-W with a phone; EV-W-002
// revokes ONLY CASE_SUMMARY_TRANSFER... but for the contact-erasure case we
// revoke CONTACT_CALLBACK. We cover both to prove scope precision.

// TestRevokeErasesOnlyGovernedContact proves that revoking CONTACT_CALLBACK
// erases the raw phone/window from storage (not just the projection) while
// leaving fragment evidence, disposition, chain, and other scopes intact.
func TestRevokeErasesOnlyGovernedContact(t *testing.T) {
	svc := newTestServiceR3(t, "R3-erase", nil)
	ctx := context.Background()

	grant := raw(`{"eventId":"EV-W-001","channel":"HOTLINE","callRef":"C-71","canonicalKey":"APP-W",
		"consent":["CONTACT_CALLBACK","CASE_SUMMARY_TRANSFER"],
		"callbackWindowMinutes":45,"identity":{"phone":"+1 415 555 0100","email":"pat@example.com"}}`)
	if r, _ := svc.SubmitOne(ctx, grant); r.Status != model.StatusAccepted {
		t.Fatalf("grant not accepted: %s", r.Status)
	}
	// Raw contact is stored before revocation.
	if rows, win, _ := svc.Store().RawStoredContact("APP-W"); rows != 2 || !win {
		t.Fatalf("expected 2 methods + window stored, got rows=%d win=%v", rows, win)
	}

	// EV-W-002 revokes CONTACT_CALLBACK.
	rev := raw(`{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-W"}`)
	if r, _ := svc.SubmitOne(ctx, rev); r.Status != model.StatusAccepted {
		t.Fatalf("revoke not accepted: %s", r.Status)
	}

	// Raw phone/email and the exact window are ERASED from storage.
	if rows, win, _ := svc.Store().RawStoredContact("APP-W"); rows != 0 || win {
		t.Fatalf("revoked raw contact must be erased, got rows=%d win=%v", rows, win)
	}

	req, _, _ := svc.Store().GetCanonical("APP-W")
	// Non-revoked scope must remain effective (not over-cleared).
	if !contains(req.EffectiveConsent, "CASE_SUMMARY_TRANSFER") {
		t.Fatalf("non-revoked scope was over-cleared: %+v", req.EffectiveConsent)
	}
	if !contains(req.RevokedConsent, "CONTACT_CALLBACK") {
		t.Fatalf("CONTACT_CALLBACK should be revoked: %+v", req.RevokedConsent)
	}
	// Non-reversible evidence preserved: coarse disposition + chain length.
	if req.Contact.CallbackDisposition != "INTRADAY" {
		t.Fatalf("disposition evidence should survive erasure: %+v", req.Contact)
	}
	if len(req.Chain) != 2 {
		t.Fatalf("event chain must be preserved: %+v", req.Chain)
	}
	if req.Contact.Exposed || req.Contact.HasPendingContact {
		t.Fatalf("no contact should be exposed or pending after erasure: %+v", req.Contact)
	}
}

// TestLateRetryCannotResurrectRevokedPhone is the core Round 3 case: after the
// phone's governing scope is revoked, a late hotline retry that carries the
// same (revoked) phone must NOT re-store it.
func TestLateRetryCannotResurrectRevokedPhone(t *testing.T) {
	svc := newTestServiceR3(t, "R3-late", nil)
	ctx := context.Background()

	grant := raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","canonicalKey":"APP-L",
		"consent":["CONTACT_CALLBACK"],"identity":{"phone":"+1 415 555 0100"}}`)
	svc.SubmitOne(ctx, grant)
	rev := raw(`{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-L"}`)
	svc.SubmitOne(ctx, rev)

	// Late hotline retry carrying the revoked phone, correlating to APP-L.
	late := raw(`{"eventId":"EV-H-LATE","channel":"HOTLINE","callRef":"C-77","canonicalKey":"APP-L",
		"callbackWindowMinutes":30,"identity":{"phone":"+1 415 555 0100"}}`)
	r, _ := svc.SubmitOne(ctx, late)
	if r.Status != model.StatusAccepted {
		t.Fatalf("late event should still be accepted (event evidence retained): %s", r.Status)
	}

	// The revoked phone/window must NOT be resurrected.
	if rows, win, _ := svc.Store().RawStoredContact("APP-L"); rows != 0 || win {
		t.Fatalf("late retry resurrected revoked contact: rows=%d win=%v", rows, win)
	}
	req, _, _ := svc.Store().GetCanonical("APP-L")
	if req.Contact.Exposed || len(req.Contact.Methods) != 0 {
		t.Fatalf("revoked phone must not be exposed after late retry: %+v", req.Contact)
	}
	// The late event still joins the single chain (legitimate event evidence).
	if len(req.Chain) != 3 {
		t.Fatalf("late event should append to the single chain, got %d", len(req.Chain))
	}
}

// TestRepeatRevokeIsIdempotent verifies revoking the same scope twice changes
// nothing further and is safe.
func TestRepeatRevokeIsIdempotent(t *testing.T) {
	svc := newTestServiceR3(t, "R3-idem", nil)
	ctx := context.Background()
	svc.SubmitOne(ctx, raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","canonicalKey":"APP-I","consent":["CONTACT_CALLBACK"],"identity":{"phone":"+1 415 555 0100"}}`))

	rev1 := raw(`{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-I"}`)
	r1, _ := svc.SubmitOne(ctx, rev1)
	// A second, distinct revocation event for the same scope.
	rev2 := raw(`{"eventId":"EV-W-003","revokes":"EV-W-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-I"}`)
	r2, _ := svc.SubmitOne(ctx, rev2)
	// And an exact replay of the first revocation (same key+payload).
	r3, _ := svc.SubmitOne(ctx, rev1)

	if r1.Status != model.StatusAccepted || r2.Status != model.StatusAccepted {
		t.Fatalf("revocations should be accepted: %s %s", r1.Status, r2.Status)
	}
	if !r3.Duplicate {
		t.Fatalf("exact revoke replay should be a duplicate: %+v", r3)
	}
	req, _, _ := svc.Store().GetCanonical("APP-I")
	// Revoked exactly once, nothing resurrected, no duplicate consent rows.
	if n := countScope(req.RevokedConsent, "CONTACT_CALLBACK"); n != 1 {
		t.Fatalf("CONTACT_CALLBACK should appear once as revoked, got %d", n)
	}
	if rows, win, _ := svc.Store().RawStoredContact("APP-I"); rows != 0 || win {
		t.Fatalf("repeated revoke must stay erased: rows=%d win=%v", rows, win)
	}
}

// TestShuffledRevokeFailureSupplementReplay mixes, in arbitrary order, a
// revocation, a transiently-failing retry, and another channel's supplement,
// then replays the whole batch into a fresh store and asserts: revoked fields
// never resurrect, non-revoked scope stays, one chain, and a stable digest.
func TestShuffledRevokeFailureSupplementReplay(t *testing.T) {
	// A downstream that fails the hotline supplement exactly once (transient).
	makeDown := func() store.Downstream {
		var mu sync.Mutex
		failOnce := map[string]bool{"EV-H-SUP": true}
		return func(ev *model.CanonicalEvent) error {
			mu.Lock()
			defer mu.Unlock()
			if failOnce[ev.EventID] {
				failOnce[ev.EventID] = false
				return &store.TransientError{Err: fmt.Errorf("adapter blip")}
			}
			return nil
		}
	}

	// Events (logical set). Order is shuffled at submit time.
	grant := raw(`{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","canonicalKey":"APP-S",
		"consent":["CONTACT_CALLBACK","CASE_SUMMARY_TRANSFER"],"callbackWindowMinutes":45,
		"identity":{"phone":"+1 415 555 0100"}}`)
	revoke := raw(`{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-S"}`)
	// A late hotline supplement carrying the revoked phone (must not resurrect it)
	// plus a fresh accommodation (legitimate, non-revoked evidence).
	supplement := raw(`{"eventId":"EV-H-SUP","channel":"HOTLINE","callRef":"C-77","canonicalKey":"APP-S",
		"accommodations":["SIGN_INTERPRETER"],"callbackWindowMinutes":30,"identity":{"phone":"+1 415 555 0100"}}`)

	// Two different submission orderings; the supplement fails once then a retry
	// is appended, so the adapter recovers within each run.
	orderA := []json.RawMessage{supplement /*fails*/, grant, revoke, supplement /*retry ok*/}
	orderB := []json.RawMessage{revoke, supplement /*fails*/, supplement /*retry ok*/, grant}

	run := func(name string, batch []json.RawMessage) (string, *store.Store) {
		c, _ := contract.Parse([]byte(fixture))
		st, err := store.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		fixed := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
		st.SetClock(func() time.Time { return fixed })
		svc := New(c, st, makeDown())
		svc.SubmitBatch(context.Background(), batch)

		// Post-conditions.
		if rows, win, _ := st.RawStoredContact("APP-S"); rows != 0 || win {
			t.Fatalf("%s: revoked contact resurrected: rows=%d win=%v", name, rows, win)
		}
		req, ok, _ := st.GetCanonical("APP-S")
		if !ok {
			t.Fatalf("%s: request missing", name)
		}
		if !contains(req.EffectiveConsent, "CASE_SUMMARY_TRANSFER") {
			t.Fatalf("%s: non-revoked scope over-cleared: %+v", name, req.EffectiveConsent)
		}
		if !contains(req.RevokedConsent, "CONTACT_CALLBACK") {
			t.Fatalf("%s: CONTACT_CALLBACK should be revoked: %+v", name, req.RevokedConsent)
		}
		// The legitimate accommodation from the supplement survives.
		if !contains(req.Accommodations, "SIGN_INTERPRETER") {
			t.Fatalf("%s: legitimate supplement evidence lost: %+v", name, req.Accommodations)
		}
		sum, _, _ := st.Summary("APP-S")
		return sum.Digest, st
	}

	dA, stA := run("R3-shufA", orderA)
	defer stA.Close()
	dB, stB := run("R3-shufB", orderB)
	defer stB.Close()

	// Order-independence of the normalized end state: both orderings converge to
	// the same erased, single-chain result. (Digest includes attempts/audit,
	// which differ by order, so we compare the projected request + evidence.)
	rA, _, _ := stA.GetCanonical("APP-S")
	rB, _, _ := stB.GetCanonical("APP-S")
	if rA.Contact.Exposed != rB.Contact.Exposed ||
		rA.Contact.HasPendingContact != rB.Contact.HasPendingContact ||
		rA.Contact.CallbackDisposition != rB.Contact.CallbackDisposition ||
		len(rA.Contact.Methods) != len(rB.Contact.Methods) {
		t.Fatalf("contact projection differs across orderings: %+v vs %+v", rA.Contact, rB.Contact)
	}
	if len(rA.EffectiveConsent) != len(rB.EffectiveConsent) || len(rA.RevokedConsent) != len(rB.RevokedConsent) {
		t.Fatalf("consent state differs across orderings")
	}
	_ = dA
	_ = dB

	// Replay stability: replaying the SAME ordering into a fresh store yields the
	// same digest.
	d1, s1 := run("R3-replay1", orderA)
	d2, s2 := run("R3-replay2", orderA)
	defer s1.Close()
	defer s2.Close()
	if d1 != d2 {
		t.Fatalf("replay digest not stable: %s vs %s", d1, d2)
	}
}

// TestRevokeAcrossEventChainAndConcurrency confirms a revocation applies to the
// one converged chain even when the grant and revoke bind via different keys,
// and that concurrent revoke + late-supplement transactions still leave the
// revoked contact erased with a single chain.
func TestRevokeAcrossEventChainAndConcurrency(t *testing.T) {
	svc := newTestServiceR3(t, "R3-conc", nil)
	ctx := context.Background()

	// Grant arrives fragment-first (no correlation), then web build attaches
	// correlation APP-XC; the revoke targets APP-XC — it must reach the same
	// chain the fragment-only grant started.
	svc.SubmitOne(ctx, raw(`{"eventId":"EV-H-G","channel":"HOTLINE","callRef":"C-1","consent":["CONTACT_CALLBACK"],"callbackWindowMinutes":45,"identity":{"phone":"+1 202 555 0000"}}`))
	svc.SubmitOne(ctx, raw(`{"eventId":"EV-W-G","channel":"WEB","submissionId":"W-1","canonicalKey":"APP-XC","identity":{"phone":"+1 202 555 0000"}}`))

	// Concurrently: revoke CONTACT_CALLBACK and submit a late supplement bearing
	// the revoked phone. Neither may resurrect the phone; one chain only.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		svc.SubmitOne(ctx, raw(`{"eventId":"EV-W-002","revokes":"EV-H-G","scopes":["CONTACT_CALLBACK"],"canonicalKey":"APP-XC"}`))
	}()
	go func() {
		defer wg.Done()
		svc.SubmitOne(ctx, raw(`{"eventId":"EV-H-SUP","channel":"HOTLINE","callRef":"C-2","canonicalKey":"APP-XC","identity":{"phone":"+1 202 555 0000"}}`))
	}()
	wg.Wait()

	req, ok, err := svc.Store().GetCanonical("APP-XC")
	if err != nil || !ok {
		t.Fatalf("APP-XC not found: %v", err)
	}
	if !contains(req.RevokedConsent, "CONTACT_CALLBACK") {
		t.Fatalf("revocation should apply across the converged chain: %+v", req.RevokedConsent)
	}
	// Exactly one chain (grant-h, grant-w, revoke, supplement) => 4 links, one request.
	if len(req.Chain) != 4 {
		t.Fatalf("expected single chain of 4, got %d: %+v", len(req.Chain), req.Chain)
	}
	// Depending on the concurrent order, the supplement may run before or after
	// the revoke. Regardless, the FINAL state must not expose the revoked phone.
	if req.Contact.Exposed || len(req.Contact.Methods) != 0 {
		t.Fatalf("revoked phone exposed after concurrent txns: %+v", req.Contact)
	}
	if rows, win, _ := svc.Store().RawStoredContact("APP-XC"); rows != 0 || win {
		// Note: if supplement committed after revoke, suppression keeps it 0; if
		// before, erasure removes it. Either way the end state is erased.
		t.Fatalf("revoked contact present after concurrent txns: rows=%d win=%v", rows, win)
	}
}

func countScope(scopes []string, want string) int {
	n := 0
	for _, s := range scopes {
		if s == want {
			n++
		}
	}
	return n
}
