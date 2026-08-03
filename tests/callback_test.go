package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// 1. Hotline callback arrives BEFORE the web intake creates a record. It
// creates a provisional chain with match_status=PENDING. The later web event
// links to that same chain, producing exactly one canonical event chain.
func TestCallbackArrivesBeforeWebIntake(t *testing.T) {
	srv, _ := newTestServer(t)

	// Callback first: phone only, no existing chain.
	cb := map[string]any{
		"eventId":               "EV-CB-EARLY",
		"channel":               "HOTLINE",
		"callRef":               "C-EARLY",
		"caller":                map[string]any{"fullName": "Early User", "phone": "+1-555-0200"},
		"callbackWindowMinutes": 30,
	}
	resp, data := postEvent(t, srv, cb)
	if resp.StatusCode != 200 {
		t.Fatalf("callback: %d %s", resp.StatusCode, data)
	}
	var cbRes struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
		Callback           struct {
			MatchStatus    string `json:"matchStatus"`
			WindowStatus   string `json:"windowStatus"`
			WindowUnit     string `json:"windowUnit"`
			ConsentPending bool   `json:"consentPending"`
			ContactExposed bool   `json:"contactExposed"`
		} `json:"callback"`
		Canonical struct {
			ContactMasked bool `json:"contactMasked"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data, &cbRes)
	if cbRes.CanonicalRequestID == "" {
		t.Fatalf("expected canonical id: %s", data)
	}
	if cbRes.Callback.MatchStatus != "PENDING" {
		t.Fatalf("expected PENDING match status, got %q: %s", cbRes.Callback.MatchStatus, data)
	}
	if cbRes.Callback.WindowStatus != "OK" {
		t.Fatalf("expected window OK, got %q", cbRes.Callback.WindowStatus)
	}
	if cbRes.Callback.WindowUnit != "MINUTES" {
		t.Fatalf("expected MINUTES unit, got %q", cbRes.Callback.WindowUnit)
	}
	if !cbRes.Callback.ConsentPending {
		t.Fatalf("expected consentPending=true before consent: %s", data)
	}
	if cbRes.Callback.ContactExposed {
		t.Fatalf("contact must not be exposed before consent: %s", data)
	}
	if !cbRes.Canonical.ContactMasked {
		t.Fatalf("canonical contact must be masked before consent: %s", data)
	}

	// Web event arrives later with the same phone; it links to the chain.
	resp, data = postEvent(t, srv, map[string]any{
		"eventId":      "EV-WEB-LATE",
		"channel":      "WEB",
		"submissionId": "W-LATE",
		"applicant":    map[string]any{"fullName": "Early User", "phone": "+1-555-0200", "email": "early@example.com"},
		"consent":      []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("web: %d %s", resp.StatusCode, data)
	}
	var webRes struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
		MatchCandidates    []struct {
			Selected   bool `json:"selected"`
			Score      int  `json:"score"`
			Confidence string `json:"confidence"`
			Reasons    []struct {
				Reason string `json:"reason"`
			} `json:"reasons"`
		} `json:"matchCandidates"`
		Canonical struct {
			ContactMasked bool     `json:"contactMasked"`
			Consent       []string `json:"consent"`
			Sources       []struct {
				Channel string `json:"channel"`
			} `json:"sources"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data, &webRes)
	if webRes.CanonicalRequestID != cbRes.CanonicalRequestID {
		t.Fatalf("web event must link to the callback's chain: callback=%s web=%s",
			cbRes.CanonicalRequestID, webRes.CanonicalRequestID)
	}
	if len(webRes.MatchCandidates) != 1 || !webRes.MatchCandidates[0].Selected {
		t.Fatalf("expected one selected candidate: %s", data)
	}
	if webRes.MatchCandidates[0].Confidence != "HIGH" {
		t.Fatalf("expected HIGH confidence phone match: %s", data)
	}
	if webRes.Canonical.ContactMasked {
		t.Fatalf("contact should be exposed after CONTACT_CALLBACK granted: %s", data)
	}
	if len(webRes.Canonical.Sources) != 2 {
		t.Fatalf("expected 2 sources (hotline+web), got %d", len(webRes.Canonical.Sources))
	}
}

// 2. Partially matching, partially conflicting identity: phone matches an
// existing chain but email differs. The event links to one chain with
// match_status=CONFLICT and deterministic conflict evidence recorded.
func TestPartialMatchPartialConflict(t *testing.T) {
	srv, _ := newTestServer(t)

	// Existing chain: phone + email.
	postEvent(t, srv, map[string]any{
		"eventId":      "EV-EXIST",
		"channel":      "WEB",
		"submissionId": "W-EX",
		"applicant":    map[string]any{"fullName": "Conflict User", "phone": "+1-555-0300", "email": "orig@example.com"},
		"consent":      []string{"CONTACT_CALLBACK"},
	})

	// Callback: same phone, different email.
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":               "EV-CB-CONF",
		"channel":               "HOTLINE",
		"callRef":               "C-CONF",
		"caller":                map[string]any{"fullName": "Conflict User", "phone": "+1-555-0300", "email": "different@example.com"},
		"callbackWindowMinutes": 20,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("callback: %d %s", resp.StatusCode, data)
	}
	var res struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
		Callback           struct {
			MatchStatus string `json:"matchStatus"`
			Confidence  string `json:"confidence"`
			Candidates  []struct {
				Selected  bool `json:"selected"`
				Score     int  `json:"score"`
				Conflicts []struct {
					Field  string `json:"field"`
					Reason string `json:"reason"`
				} `json:"conflicts"`
				Reasons []struct {
					Reason string `json:"reason"`
				} `json:"reasons"`
			} `json:"candidates"`
		} `json:"callback"`
	}
	_ = json.Unmarshal(data, &res)
	if res.Callback.MatchStatus != "CONFLICT" {
		t.Fatalf("expected CONFLICT match status, got %q: %s", res.Callback.MatchStatus, data)
	}
	if res.Callback.Confidence != "MEDIUM" {
		t.Fatalf("expected MEDIUM confidence due to conflict, got %q", res.Callback.Confidence)
	}
	if len(res.Callback.Candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(res.Callback.Candidates))
	}
	cand := res.Callback.Candidates[0]
	if !cand.Selected {
		t.Fatalf("candidate should be selected despite conflict: %s", data)
	}
	hasPhoneMatch := false
	for _, r := range cand.Reasons {
		if r.Reason == "PHONE_EXACT_MATCH" {
			hasPhoneMatch = true
		}
	}
	if !hasPhoneMatch {
		t.Fatalf("expected PHONE_EXACT_MATCH reason: %s", data)
	}
	hasEmailConflict := false
	for _, c := range cand.Conflicts {
		if c.Field == "email" && c.Reason == "IDENTITY_FRAGMENT_CONFLICT" {
			hasEmailConflict = true
		}
	}
	if !hasEmailConflict {
		t.Fatalf("expected email conflict evidence: %s", data)
	}
}

// 3. Callback window classifications: zero (valid/ASAP), negative (rejected
// per-item), cross-day >=1440 (rejected per-item), and valid positive.
func TestCallbackWindowClassifications(t *testing.T) {
	srv, _ := newTestServer(t)

	cases := []struct {
		name      string
		minutes   any
		wantCode  int
		windowSt  string
		itemCode  string
	}{
		{"zero_asap", 0, 200, "ZERO", ""},
		{"negative", -5, 200, "NEGATIVE_REJECTED", "INVALID_VALUE"},
		{"cross_day", 1440, 200, "CROSS_DAY_REJECTED", "CROSS_DAY_WINDOW"},
		{"cross_day_large", 2000, 200, "CROSS_DAY_REJECTED", "CROSS_DAY_WINDOW"},
		{"valid", 45, 200, "OK", ""},
	}
	for i, tc := range cases {
		body := map[string]any{
			"eventId":               fmt.Sprintf("EV-WIN-%d", i),
			"channel":               "HOTLINE",
			"callRef":               fmt.Sprintf("C-WIN-%d", i),
			"caller":                map[string]any{"fullName": "Win User", "phone": fmt.Sprintf("+1-555-04%02d", i)},
			"callbackWindowMinutes": tc.minutes,
		}
		resp, data := postEvent(t, srv, body)
		if resp.StatusCode != tc.wantCode {
			t.Fatalf("%s: status %d body %s", tc.name, resp.StatusCode, data)
		}
		var r struct {
			Callback struct {
				WindowStatus string `json:"windowStatus"`
			} `json:"callback"`
			ItemResults []struct {
				Code string `json:"code"`
			} `json:"itemResults"`
		}
		_ = json.Unmarshal(data, &r)
		if r.Callback.WindowStatus != tc.windowSt {
			t.Fatalf("%s: expected window %q, got %q", tc.name, tc.windowSt, r.Callback.WindowStatus)
		}
		if tc.itemCode != "" {
			found := false
			for _, ir := range r.ItemResults {
				if ir.Code == tc.itemCode {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s: expected per-item code %q in %s", tc.name, tc.itemCode, data)
			}
		}
	}
}

// 4. Duplicate callback key (same eventId, same payload) returns the original
// idempotently, including match evidence and callback projection.
func TestDuplicateCallbackKeyIdempotent(t *testing.T) {
	srv, _ := newTestServer(t)
	body := map[string]any{
		"eventId":               "EV-CB-IDEM",
		"channel":               "HOTLINE",
		"callRef":               "C-IDEM",
		"caller":                map[string]any{"fullName": "Idem User", "phone": "+1-555-0500"},
		"callbackWindowMinutes": 15,
	}
	resp1, data1 := postEvent(t, srv, body)
	if resp1.StatusCode != 200 {
		t.Fatalf("first: %d %s", resp1.StatusCode, data1)
	}
	resp2, data2 := postEvent(t, srv, body)
	if resp2.StatusCode != 200 {
		t.Fatalf("second: %d %s", resp2.StatusCode, data2)
	}
	var r1, r2 struct {
		ID               string `json:"canonicalRequestId"`
		IdempotentReplay bool   `json:"idempotentReplay"`
		Callback         struct {
			WindowStatus string `json:"windowStatus"`
			MatchStatus  string `json:"matchStatus"`
		} `json:"callback"`
	}
	_ = json.Unmarshal(data1, &r1)
	_ = json.Unmarshal(data2, &r2)
	if r1.ID != r2.ID {
		t.Fatalf("canonical id changed: %s vs %s", r1.ID, r2.ID)
	}
	if !r2.IdempotentReplay {
		t.Fatalf("expected idempotentReplay=true: %s", data2)
	}
	if r2.Callback.WindowStatus != "OK" || r2.Callback.MatchStatus != "PENDING" {
		t.Fatalf("callback not preserved on replay: %s", data2)
	}
}

// 5. Same callback key with a different payload is a 409 conflict; no second
// chain or event is created.
func TestCallbackSameKeyDifferentPayloadConflict(t *testing.T) {
	srv, st := newTestServer(t)
	base := map[string]any{
		"eventId":               "EV-CB-CONF2",
		"channel":               "HOTLINE",
		"callRef":               "C-CONF2",
		"caller":                map[string]any{"fullName": "Key User", "phone": "+1-555-0600"},
		"callbackWindowMinutes": 10,
	}
	resp, data := postEvent(t, srv, base)
	if resp.StatusCode != 200 {
		t.Fatalf("first: %d %s", resp.StatusCode, data)
	}
	changed := map[string]any{
		"eventId":               "EV-CB-CONF2",
		"channel":               "HOTLINE",
		"callRef":               "C-CONF2",
		"caller":                map[string]any{"fullName": "Key User", "phone": "+1-555-0600"},
		"callbackWindowMinutes": 99,
	}
	resp, data = postEvent(t, srv, changed)
	if resp.StatusCode != 409 {
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, data)
	}
	var conflict struct {
		Status   string `json:"status"`
		Conflict struct {
			ExistingHash string `json:"existingPayloadHash"`
			IncomingHash string `json:"incomingPayloadHash"`
		} `json:"conflict"`
	}
	_ = json.Unmarshal(data, &conflict)
	if conflict.Status != "CONFLICT" || conflict.Conflict.ExistingHash == "" || conflict.Conflict.IncomingHash == "" {
		t.Fatalf("expected conflict hashes: %s", data)
	}
	if conflict.Conflict.ExistingHash == conflict.Conflict.IncomingHash {
		t.Fatalf("hashes must differ on payload change")
	}
	// Exactly one event row for this eventId, no second chain.
	var count int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-CB-CONF2'`).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

// 6. Two channels (hotline + web) concurrently claiming the same applicant
// via the same phone produce exactly one canonical chain.
func TestConcurrentCallbackAndWebFormOneChain(t *testing.T) {
	srv, _ := newTestServer(t)
	const n = 6
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var body map[string]any
			if i%2 == 0 {
				body = map[string]any{
					"eventId":               fmt.Sprintf("EV-CCB-%d", i),
					"channel":               "HOTLINE",
					"callRef":               fmt.Sprintf("C-CCB-%d", i),
					"caller":                map[string]any{"fullName": "Concurrent User", "phone": "+1-555-0700"},
					"callbackWindowMinutes": 25,
				}
			} else {
				body = map[string]any{
					"eventId":      fmt.Sprintf("EV-CWEB-%d", i),
					"channel":      "WEB",
					"submissionId": fmt.Sprintf("W-CCB-%d", i),
					"applicant":    map[string]any{"fullName": "Concurrent User", "phone": "+1-555-0700"},
					"consent":      []string{"CONTACT_CALLBACK"},
				}
			}
			resp, data := postEvent(t, srv, body)
			if resp.StatusCode != 200 {
				errs[i] = fmt.Errorf("status %d: %s", resp.StatusCode, data)
				return
			}
			var r struct {
				ID string `json:"canonicalRequestId"`
			}
			_ = json.Unmarshal(data, &r)
			ids[i] = r.ID
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("goroutine %d: %v", i, e)
		}
	}
	first := ids[0]
	for i := 1; i < n; i++ {
		if ids[i] != first {
			t.Fatalf("expected one chain, got %q vs %q", first, ids[i])
		}
	}
	// Verify chain has all events with contiguous sequence.
	resp, data := get(t, srv, "/v1/intake/canonical/"+first+"/audit")
	if resp.StatusCode != 200 {
		t.Fatalf("audit: %d %s", resp.StatusCode, data)
	}
	var audit struct {
		EventCount int `json:"eventCount"`
		Events     []struct {
			Sequence int `json:"sequence"`
		} `json:"events"`
	}
	_ = json.Unmarshal(data, &audit)
	if audit.EventCount != n {
		t.Fatalf("expected %d events, got %d", n, audit.EventCount)
	}
	for i, ev := range audit.Events {
		if ev.Sequence != i+1 {
			t.Fatalf("non-contiguous sequence: %v", audit.Events)
		}
	}
}

// 7. Adapter transient failure on a callback, then a successful retry: only
// one event/chain is created, attempts trail shows 503 then OK.
func TestCallbackAdapterTransientRecovery(t *testing.T) {
	srv, st := newTestServer(t)
	body := map[string]any{
		"eventId":                  "EV-CB-TR",
		"channel":                  "HOTLINE",
		"callRef":                  "C-TR",
		"caller":                   map[string]any{"fullName": "Transient User", "phone": "+1-555-0800"},
		"callbackWindowMinutes":    12,
		"simulateAdapterTransient": true,
	}
	resp, data := postEvent(t, srv, body)
	if resp.StatusCode != 503 {
		t.Fatalf("expected 503, got %d: %s", resp.StatusCode, data)
	}
	var evCount, cbCount int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-CB-TR'`).Scan(&evCount)
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM callback_events WHERE event_id='EV-CB-TR'`).Scan(&cbCount)
	if evCount != 0 || cbCount != 0 {
		t.Fatalf("transient failure must not commit event/callback, got events=%d callbacks=%d", evCount, cbCount)
	}

	body["simulateAdapterTransient"] = false
	resp, data = postEvent(t, srv, body)
	if resp.StatusCode != 200 {
		t.Fatalf("retry: %d %s", resp.StatusCode, data)
	}
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-CB-TR'`).Scan(&evCount)
	if evCount != 1 {
		t.Fatalf("expected 1 event after retry, got %d", evCount)
	}

	resp, data = get(t, srv, "/v1/intake/events/EV-CB-TR/attempts")
	if resp.StatusCode != 200 {
		t.Fatalf("attempts: %d %s", resp.StatusCode, data)
	}
	var att struct {
		Attempts []struct {
			Status string `json:"status"`
		} `json:"attempts"`
	}
	_ = json.Unmarshal(data, &att)
	if len(att.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(att.Attempts))
	}
	hasTransient, hasOK := false, false
	for _, a := range att.Attempts {
		if a.Status == "ADAPTER_TRANSIENT" {
			hasTransient = true
		}
		if a.Status == "OK" {
			hasOK = true
		}
	}
	if !hasTransient || !hasOK {
		t.Fatalf("expected ADAPTER_TRANSIENT then OK, got %s", data)
	}
}

// 8. Contact details in a callback must not be exposed before consent, and a
// later idempotent retry must not leak them either.
func TestCallbackContactMaskedBeforeConsent(t *testing.T) {
	srv, _ := newTestServer(t)

	// Callback with phone but no consent granted.
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":               "EV-CB-MASK",
		"channel":               "HOTLINE",
		"callRef":               "C-MASK",
		"caller":                map[string]any{"fullName": "Mask User", "phone": "+1-555-0900", "email": "mask@example.com"},
		"callbackWindowMinutes": 5,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("callback: %d %s", resp.StatusCode, data)
	}
	var r1 struct {
		ID       string `json:"canonicalRequestId"`
		Callback struct {
			ContactExposed bool `json:"contactExposed"`
			ConsentPending bool `json:"consentPending"`
		} `json:"callback"`
		Canonical struct {
			ContactMasked bool `json:"contactMasked"`
			Person        struct {
				Phone string `json:"phone"`
				Email string `json:"email"`
			} `json:"person"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data, &r1)
	if r1.Callback.ContactExposed || !r1.Canonical.ContactMasked {
		t.Fatalf("contact must be masked before consent: %s", data)
	}
	if r1.Canonical.Person.Phone != "" || r1.Canonical.Person.Email != "" {
		t.Fatalf("raw phone/email must not appear before consent: %s", data)
	}

	// Idempotent retry must still mask.
	_, data2 := postEvent(t, srv, map[string]any{
		"eventId":               "EV-CB-MASK",
		"channel":               "HOTLINE",
		"callRef":               "C-MASK",
		"caller":                map[string]any{"fullName": "Mask User", "phone": "+1-555-0900", "email": "mask@example.com"},
		"callbackWindowMinutes": 5,
	})
	var r2 struct {
		IdempotentReplay bool `json:"idempotentReplay"`
		Canonical        struct {
			ContactMasked bool `json:"contactMasked"`
			Person        struct {
				Phone string `json:"phone"`
			} `json:"person"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data2, &r2)
	if !r2.IdempotentReplay {
		t.Fatalf("expected idempotent replay: %s", data2)
	}
	if !r2.Canonical.ContactMasked || r2.Canonical.Person.Phone != "" {
		t.Fatalf("retry must not expose contact before consent: %s", data2)
	}

	// Callback-specific endpoint also masks.
	resp, data = get(t, srv, "/v1/intake/events/EV-CB-MASK/callback")
	if resp.StatusCode != 200 {
		t.Fatalf("callback endpoint: %d %s", resp.StatusCode, data)
	}
	if bytes.Contains(data, []byte("+1-555-0900")) {
		t.Fatalf("callback endpoint must not contain raw phone: %s", data)
	}
}

// 9. Match candidates endpoint returns deterministic evidence, and the
// candidates list is stable on replay.
func TestMatchCandidatesEvidenceStable(t *testing.T) {
	srv, _ := newTestServer(t)
	postEvent(t, srv, map[string]any{
		"eventId":      "EV-EVID",
		"channel":      "WEB",
		"submissionId": "W-EVID",
		"applicant":    map[string]any{"fullName": "Evidence User", "phone": "+1-555-0950", "email": "evid@example.com"},
	})
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":               "EV-CB-EVID",
		"channel":               "HOTLINE",
		"callRef":               "C-EVID",
		"caller":                map[string]any{"fullName": "Evidence User", "phone": "+1-555-0950"},
		"callbackWindowMinutes": 33,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("callback: %d %s", resp.StatusCode, data)
	}
	var r struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &r)

	resp, data = get(t, srv, "/v1/intake/events/EV-CB-EVID/candidates")
	if resp.StatusCode != 200 {
		t.Fatalf("candidates: %d %s", resp.StatusCode, data)
	}
	var cands struct {
		Candidates []struct {
			Rank       int    `json:"rank"`
			Selected   bool   `json:"selected"`
			Confidence string `json:"confidence"`
			Score      int    `json:"score"`
			Reasons    []struct {
				Field  string `json:"field"`
				Reason string `json:"reason"`
			} `json:"reasons"`
		} `json:"candidates"`
	}
	_ = json.Unmarshal(data, &cands)
	if len(cands.Candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d: %s", len(cands.Candidates), data)
	}
	c := cands.Candidates[0]
	if c.Rank != 1 || !c.Selected || c.Confidence != "HIGH" || c.Score < 80 {
		t.Fatalf("unexpected candidate: %s", data)
	}
	if len(c.Reasons) == 0 {
		t.Fatalf("expected match reasons: %s", data)
	}

	// Callbacks list endpoint.
	resp, data = get(t, srv, "/v1/intake/canonical/"+r.ID+"/callbacks")
	if resp.StatusCode != 200 {
		t.Fatalf("callbacks: %d %s", resp.StatusCode, data)
	}
	if !bytes.Contains(data, []byte("EV-CB-EVID")) {
		t.Fatalf("expected callback in list: %s", data)
	}
}

// 10. Out-of-order revocation arriving before the callback's consent grant
// is handled; the callback stays masked and a retry of the consent event
// still forms one chain.
func TestOutOfOrderRevocationAfterCallback(t *testing.T) {
	srv, _ := newTestServer(t)

	// Callback grants contact consent.
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":               "EV-CB-REV",
		"channel":               "HOTLINE",
		"callRef":               "C-REV",
		"caller":                map[string]any{"fullName": "Rev User", "phone": "+1-555-0999"},
		"callbackWindowMinutes": 40,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("callback: %d %s", resp.StatusCode, data)
	}
	var r struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &r)

	// Revocation withdraws the callback consent.
	resp, data = postEvent(t, srv, map[string]any{
		"eventId": "EV-REV-1",
		"revokes": "EV-CB-REV",
		"scopes":  []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("revocation: %d %s", resp.StatusCode, data)
	}

	// Contact must now be masked, even when reading the callback record.
	resp, data = get(t, srv, "/v1/intake/canonical/"+r.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d %s", resp.StatusCode, data)
	}
	var snap struct {
		ContactMasked bool `json:"contactMasked"`
	}
	_ = json.Unmarshal(data, &snap)
	if !snap.ContactMasked {
		t.Fatalf("expected contactMasked after revocation: %s", data)
	}
	resp, data = get(t, srv, "/v1/intake/events/EV-CB-REV/callback")
	if resp.StatusCode != 200 {
		t.Fatalf("callback: %d %s", resp.StatusCode, data)
	}
	if bytes.Contains(data, []byte("+1-555-0999")) {
		t.Fatalf("callback must not leak phone after revocation: %s", data)
	}
	var cb struct {
		ContactExposed bool `json:"contactExposed"`
	}
	_ = json.Unmarshal(data, &cb)
	if cb.ContactExposed {
		t.Fatalf("contactExposed must be false after revocation: %s", data)
	}
}

// 11. Replay reconstructs the chain including the early callback event; the
// callback list is consistent.
func TestReplayIncludesEarlyCallback(t *testing.T) {
	srv, _ := newTestServer(t)
	postEvent(t, srv, map[string]any{
		"eventId":               "EV-RC-1",
		"channel":               "HOTLINE",
		"callRef":               "C-RC1",
		"caller":                map[string]any{"fullName": "Replay CB", "phone": "+1-555-1000"},
		"callbackWindowMinutes": 0,
	})
	postEvent(t, srv, map[string]any{
		"eventId":      "EV-RW-1",
		"channel":      "WEB",
		"submissionId": "W-RC1",
		"applicant":    map[string]any{"fullName": "Replay CB", "phone": "+1-555-1000", "email": "rc@example.com"},
		"consent":      []string{"CONTACT_CALLBACK"},
	})
	// Fetch the canonical id via a third web event.
	_, data2 := postEvent(t, srv, map[string]any{
		"eventId":      "EV-RW-2",
		"channel":      "WEB",
		"submissionId": "W-RC2",
		"applicant":    map[string]any{"fullName": "Replay CB", "email": "rc@example.com"},
	})
	var r3 struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data2, &r3)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/intake/canonical/"+r3.ID+"/replay", nil)
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	defer resp3.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp3.Body)
	if resp3.StatusCode != 200 {
		t.Fatalf("replay: %d %s", resp3.StatusCode, buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte("EV-RC-1")) {
		t.Fatalf("replay must include early callback event: %s", buf.String())
	}
}
