package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// 1. Fixture EV-W-002 revokes CASE_SUMMARY_TRANSFER from EV-W-001.
// The full_name field is cleared but event evidence (id, channel, source,
// hash, sequence) is preserved.
func TestFixtureEventWW002RevokesCaseSummary(t *testing.T) {
	srv, st := newTestServer(t)

	postEvent(t, srv, map[string]any{
		"eventId":      "EV-W-001",
		"channel":      "WEB",
		"submissionId": "W-71",
		"applicant":    map[string]any{"fullName": "Jane Doe", "email": "jane@example.com", "phone": "+1-555-0100"},
		"consent":      []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"},
		"accommodations": []string{"BRAILLE_MATERIAL"},
	})

	resp, data := postEvent(t, srv, map[string]any{
		"eventId": "EV-W-002",
		"revokes": "EV-W-001",
		"scopes":  []string{"CASE_SUMMARY_TRANSFER"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("revocation: %d %s", resp.StatusCode, data)
	}

	var cr struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &cr)

	resp, data = get(t, srv, "/v1/intake/canonical/"+cr.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d %s", resp.StatusCode, data)
	}
	var snap struct {
		Person struct {
			FullName string `json:"fullName"`
			Phone    string `json:"phone"`
			Email    string `json:"email"`
		} `json:"person"`
		Consent        []string `json:"consent"`
		Accommodations []string `json:"accommodations"`
		RevokedFields  []string `json:"revokedFields"`
		ContactMasked  bool     `json:"contactMasked"`
		Sources        []struct {
			Channel  string `json:"channel"`
			SourceID string `json:"sourceId"`
		} `json:"sources"`
	}
	_ = json.Unmarshal(data, &snap)

	if snap.Person.FullName != "" {
		t.Fatalf("fullName must be cleared after CASE_SUMMARY_TRANSFER revocation, got %q", snap.Person.FullName)
	}
	if snap.Person.Phone != "+1-555-0100" || snap.Person.Email != "jane@example.com" {
		t.Fatalf("non-revoked contact must remain: phone=%q email=%q", snap.Person.Phone, snap.Person.Email)
	}
	if snap.ContactMasked {
		t.Fatalf("CONTACT_CALLBACK still granted, contact should not be masked")
	}
	if len(snap.Accommodations) != 1 || snap.Accommodations[0] != "BRAILLE_MATERIAL" {
		t.Fatalf("non-revoked accommodation must remain: %v", snap.Accommodations)
	}
	if !containsStr(snap.RevokedFields, "full_name") {
		t.Fatalf("expected full_name in revokedFields, got %v", snap.RevokedFields)
	}
	if containsStr(snap.RevokedFields, "phone") || containsStr(snap.RevokedFields, "accommodations") {
		t.Fatalf("non-revoked fields must not appear in revokedFields: %v", snap.RevokedFields)
	}
	if len(snap.Consent) != 2 {
		t.Fatalf("expected 2 remaining consent scopes, got %v", snap.Consent)
	}
	if len(snap.Sources) != 1 || snap.Sources[0].SourceID != "W-71" {
		t.Fatalf("event evidence (source) must be preserved: %v", snap.Sources)
	}

	var eventCount, revokeCount int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-W-001'`).Scan(&eventCount)
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM consent_records WHERE canonical_request_id=? AND scope='CASE_SUMMARY_TRANSFER' AND action='REVOKE'`, cr.ID).Scan(&revokeCount)
	if eventCount != 1 || revokeCount != 1 {
		t.Fatalf("event evidence and REVOKE record must be preserved: events=%d revokes=%d", eventCount, revokeCount)
	}
}

// 2. Late hotline retry carries an already-revoked phone number. Both an
// idempotent retry of the original hotline event and a NEW hotline event with
// the same phone must not re-expose it.
func TestLateHotlineRetryWithRevokedPhone(t *testing.T) {
	srv, _ := newTestServer(t)

	hotlineBody := map[string]any{
		"eventId":               "EV-H-REV",
		"channel":               "HOTLINE",
		"callRef":               "C-REV",
		"caller":                map[string]any{"fullName": "Phone User", "phone": "+1-555-0200"},
		"callbackWindowMinutes": 20,
		"consent":               []string{"CONTACT_CALLBACK"},
	}
	resp, data := postEvent(t, srv, hotlineBody)
	if resp.StatusCode != 200 {
		t.Fatalf("hotline: %d %s", resp.StatusCode, data)
	}
	var r1 struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &r1)

	resp, data = postEvent(t, srv, map[string]any{
		"eventId": "EV-REV-CB",
		"revokes": "EV-H-REV",
		"scopes":  []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("revocation: %d %s", resp.StatusCode, data)
	}

	resp, data = get(t, srv, "/v1/intake/canonical/"+r1.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d %s", resp.StatusCode, data)
	}
	var snap struct {
		Person struct {
			Phone string `json:"phone"`
		} `json:"person"`
		ContactMasked bool     `json:"contactMasked"`
		RevokedFields []string `json:"revokedFields"`
	}
	_ = json.Unmarshal(data, &snap)
	if !snap.ContactMasked || snap.Person.Phone != "" {
		t.Fatalf("phone must be masked after revocation: %s", data)
	}
	if !containsStr(snap.RevokedFields, "phone") {
		t.Fatalf("expected phone revoked: %v", snap.RevokedFields)
	}

	// Idempotent retry of the original hotline event must not revive phone.
	_, data2 := postEvent(t, srv, hotlineBody)
	var retry struct {
		IdempotentReplay bool `json:"idempotentReplay"`
		Canonical        struct {
			Person struct {
				Phone string `json:"phone"`
			} `json:"person"`
			ContactMasked bool `json:"contactMasked"`
		} `json:"canonical"`
		Callback struct {
			ContactExposed bool `json:"contactExposed"`
		} `json:"callback"`
	}
	_ = json.Unmarshal(data2, &retry)
	if !retry.IdempotentReplay {
		t.Fatalf("expected idempotent replay: %s", data2)
	}
	if retry.Canonical.Person.Phone != "" || !retry.Canonical.ContactMasked {
		t.Fatalf("retry must not revive revoked phone: %s", data2)
	}
	if retry.Callback.ContactExposed {
		t.Fatalf("callback contactExposed must be false after revocation: %s", data2)
	}

	// A NEW hotline event carrying the same phone must also not revive it.
	resp, data = postEvent(t, srv, map[string]any{
		"eventId":               "EV-H-NEW",
		"channel":               "HOTLINE",
		"callRef":               "C-NEW",
		"caller":                map[string]any{"fullName": "Phone User", "phone": "+1-555-0200"},
		"callbackWindowMinutes": 10,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("new hotline: %d %s", resp.StatusCode, data)
	}
	var nr struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
		Canonical          struct {
			Person struct {
				Phone string `json:"phone"`
			} `json:"person"`
			ContactMasked bool `json:"contactMasked"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data, &nr)
	if nr.CanonicalRequestID != r1.ID {
		t.Fatalf("new event must link to same chain: %s vs %s", nr.CanonicalRequestID, r1.ID)
	}
	if nr.Canonical.Person.Phone != "" || !nr.Canonical.ContactMasked {
		t.Fatalf("new event with revoked phone must not expose it: %s", data)
	}
}

// 3. Out-of-order: revocation arrives, then a failed adapter retry, then a
// web supplementation. After replay, revoked fields stay cleared, non-revoked
// fields are supplemented, and the chain remains singular.
func TestOutOfOrderRevokeFailSupplement(t *testing.T) {
	srv, _ := newTestServer(t)

	postEvent(t, srv, map[string]any{
		"eventId":        "EV-OO-1",
		"channel":        "WEB",
		"submissionId":   "W-OO1",
		"applicant":      map[string]any{"fullName": "OO User", "phone": "+1-555-0300", "email": "oo@example.com"},
		"consent":        []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"},
		"accommodations": []string{"SIGN_INTERPRETER"},
	})

	// Revoke only CONTACT_CALLBACK: phone and email must be cleared, but
	// fullName and accommodations must remain.
	postEvent(t, srv, map[string]any{
		"eventId": "EV-OO-REV",
		"revokes": "EV-OO-1",
		"scopes":  []string{"CONTACT_CALLBACK"},
	})

	// Failed adapter retry (transient).
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":                  "EV-OO-2",
		"channel":                  "PHYSICAL",
		"deskReceiptNo":            "D-OO",
		"visitor":                  map[string]any{"fullName": "OO User", "phone": "+1-555-0300"},
		"simulateAdapterTransient": true,
	})
	if resp.StatusCode != 503 {
		t.Fatalf("expected 503 transient, got %d: %s", resp.StatusCode, data)
	}

	// Successful supplementation from physical channel (no phone, adds ref).
	resp, data = postEvent(t, srv, map[string]any{
		"eventId":       "EV-OO-2",
		"channel":       "PHYSICAL",
		"deskReceiptNo": "D-OO",
		"visitor":       map[string]any{"fullName": "OO User", "referenceNumber": "REF-123"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("supplement: %d %s", resp.StatusCode, data)
	}
	var sr struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &sr)

	resp, data = get(t, srv, "/v1/intake/canonical/"+sr.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d %s", resp.StatusCode, data)
	}
	var snap struct {
		Person struct {
			Phone           string `json:"phone"`
			Email           string `json:"email"`
			FullName        string `json:"fullName"`
			ReferenceNumber string `json:"referenceNumber"`
		} `json:"person"`
		ContactMasked bool     `json:"contactMasked"`
		RevokedFields []string `json:"revokedFields"`
		Accommodations []string `json:"accommodations"`
		Consent       []string `json:"consent"`
		Sequence      int      `json:"sequence"`
	}
	_ = json.Unmarshal(data, &snap)
	if snap.Person.Phone != "" || !snap.ContactMasked {
		t.Fatalf("phone must stay revoked after supplementation: %s", data)
	}
	if snap.Person.Email != "" {
		t.Fatalf("email (also governed by CONTACT_CALLBACK) must be cleared: %q", snap.Person.Email)
	}
	if snap.Person.FullName != "OO User" {
		t.Fatalf("fullName (governed by non-revoked CASE_SUMMARY_TRANSFER) must remain: %q", snap.Person.FullName)
	}
	if snap.Person.ReferenceNumber != "REF-123" {
		t.Fatalf("referenceNumber supplementation should be stored: %q", snap.Person.ReferenceNumber)
	}
	if len(snap.Accommodations) != 1 || snap.Accommodations[0] != "SIGN_INTERPRETER" {
		t.Fatalf("non-revoked accommodation must remain: %v", snap.Accommodations)
	}
	if len(snap.Consent) != 2 {
		t.Fatalf("expected 2 consent scopes after revoking CONTACT_CALLBACK, got %v", snap.Consent)
	}
	if !containsStr(snap.RevokedFields, "phone") {
		t.Fatalf("phone must be in revokedFields: %v", snap.RevokedFields)
	}
	if snap.Sequence != 3 {
		t.Fatalf("expected 3 events (original + revocation + supplement), got sequence %d", snap.Sequence)
	}

	// Replay must produce the same result.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/intake/canonical/"+sr.ID+"/replay", nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	defer resp2.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp2.Body)
	if !bytes.Contains(buf.Bytes(), []byte(`"contactMasked": true`)) {
		t.Fatalf("replay must preserve revoked state: %s", buf.String())
	}
	if bytes.Contains(buf.Bytes(), []byte("+1-555-0300")) {
		t.Fatalf("replay must not contain raw revoked phone: %s", buf.String())
	}
}

// 4. Duplicate revocation (same eventId, same payload) is idempotent: no
// duplicate REVOKE records and original result returned.
func TestDuplicateRevocationIdempotent(t *testing.T) {
	srv, st := newTestServer(t)

	postEvent(t, srv, map[string]any{
		"eventId":      "EV-DUP-1",
		"channel":      "WEB",
		"submissionId": "W-DUP",
		"applicant":    map[string]any{"fullName": "Dup User", "phone": "+1-555-0400"},
		"consent":      []string{"CONTACT_CALLBACK"},
	})

	revBody := map[string]any{
		"eventId": "EV-DUP-REV",
		"revokes": "EV-DUP-1",
		"scopes":  []string{"CONTACT_CALLBACK"},
	}
	resp1, _ := postEvent(t, srv, revBody)
	if resp1.StatusCode != 200 {
		t.Fatalf("first revocation: %d", resp1.StatusCode)
	}
	resp2, data2 := postEvent(t, srv, revBody)
	if resp2.StatusCode != 200 {
		t.Fatalf("duplicate revocation: %d %s", resp2.StatusCode, data2)
	}
	var r2 struct {
		IdempotentReplay bool `json:"idempotentReplay"`
	}
	_ = json.Unmarshal(data2, &r2)
	if !r2.IdempotentReplay {
		t.Fatalf("duplicate revocation should be idempotent replay: %s", data2)
	}

	var revokeCount int
	var canonicalID string
	_ = st.DB().QueryRow(`SELECT canonical_request_id FROM events WHERE event_id='EV-DUP-1'`).Scan(&canonicalID)
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM consent_records WHERE canonical_request_id=? AND scope='CONTACT_CALLBACK' AND action='REVOKE'`, canonicalID).Scan(&revokeCount)
	if revokeCount != 1 {
		t.Fatalf("expected exactly 1 REVOKE record, got %d", revokeCount)
	}
	var revokedFieldCount int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM revoked_fields WHERE canonical_request_id=? AND field='phone'`, canonicalID).Scan(&revokedFieldCount)
	if revokedFieldCount != 1 {
		t.Fatalf("expected exactly 1 revoked_fields row, got %d", revokedFieldCount)
	}
}

// 5. Revoking ACCOMMODATION_TRANSFER removes accommodations but does not
// affect contact or name fields.
func TestRevokeAccommodationOnly(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, data := postEvent(t, srv, map[string]any{
		"eventId":        "EV-ACC-1",
		"channel":        "WEB",
		"submissionId":   "W-ACC",
		"applicant":      map[string]any{"fullName": "Acc User", "phone": "+1-555-0500", "email": "acc@example.com"},
		"consent":        []string{"CONTACT_CALLBACK", "ACCOMMODATION_TRANSFER"},
		"accommodations": []string{"STEP_FREE_ACCESS", "TEXT_ONLY"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("event: %d %s", resp.StatusCode, data)
	}

	postEvent(t, srv, map[string]any{
		"eventId": "EV-ACC-REV",
		"revokes": "EV-ACC-1",
		"scopes":  []string{"ACCOMMODATION_TRANSFER"},
	})

	var cr struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &cr)
	resp, data = get(t, srv, "/v1/intake/canonical/"+cr.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d", resp.StatusCode)
	}
	var snap struct {
		Person struct {
			Phone string `json:"phone"`
			Email string `json:"email"`
		} `json:"person"`
		Accommodations []string `json:"accommodations"`
		ContactMasked  bool     `json:"contactMasked"`
		RevokedFields  []string `json:"revokedFields"`
	}
	_ = json.Unmarshal(data, &snap)
	if len(snap.Accommodations) != 0 {
		t.Fatalf("accommodations must be cleared, got %v", snap.Accommodations)
	}
	if snap.Person.Phone != "+1-555-0500" || snap.ContactMasked {
		t.Fatalf("contact must remain since CONTACT_CALLBACK not revoked: %s", data)
	}
	if !containsStr(snap.RevokedFields, "accommodations") {
		t.Fatalf("expected accommodations revoked: %v", snap.RevokedFields)
	}

	// A later event requesting a new accommodation must NOT repopulate it.
	resp, _ = postEvent(t, srv, map[string]any{
		"eventId":        "EV-ACC-2",
		"channel":        "WEB",
		"submissionId":   "W-ACC2",
		"applicant":      map[string]any{"fullName": "Acc User", "email": "acc@example.com"},
		"accommodations": []string{"BRAILLE_MATERIAL"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("second event: %d", resp.StatusCode)
	}
	resp, data = get(t, srv, "/v1/intake/canonical/"+cr.ID)
	var snap2 struct {
		Accommodations []string `json:"accommodations"`
	}
	_ = json.Unmarshal(data, &snap2)
	if len(snap2.Accommodations) != 0 {
		t.Fatalf("revoked accommodations must not be repopulated by later event: %v", snap2.Accommodations)
	}
}

// 6. Concurrent revocation of the same scope from two control events must not
// create duplicate REVOKE records or a second chain.
func TestConcurrentRevocationNoDuplicates(t *testing.T) {
	srv, st := newTestServer(t)

	postEvent(t, srv, map[string]any{
		"eventId":      "EV-CR-1",
		"channel":      "WEB",
		"submissionId": "W-CR",
		"applicant":    map[string]any{"fullName": "Conc Rev User", "phone": "+1-555-0600"},
		"consent":      []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER"},
	})

	const n = 4
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := map[string]any{
				"eventId": fmt.Sprintf("EV-CR-REV-%d", i),
				"revokes": "EV-CR-1",
				"scopes":  []string{"CASE_SUMMARY_TRANSFER"},
			}
			resp, _ := postEvent(t, srv, body)
			if resp.StatusCode != 200 {
				errs[i] = fmt.Errorf("status %d", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("goroutine %d: %v", i, e)
		}
	}

	var canonicalID string
	_ = st.DB().QueryRow(`SELECT canonical_request_id FROM events WHERE event_id='EV-CR-1'`).Scan(&canonicalID)

	var eventCount, revokeCount int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE canonical_request_id=?`, canonicalID).Scan(&eventCount)
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM consent_records WHERE canonical_request_id=? AND scope='CASE_SUMMARY_TRANSFER' AND action='REVOKE'`, canonicalID).Scan(&revokeCount)
	if eventCount != 1+n {
		t.Fatalf("expected %d events (1 original + %d revocations), got %d", 1+n, n, eventCount)
	}
	if revokeCount != n {
		t.Fatalf("expected %d REVOKE records (one per distinct revocation event), got %d", n, revokeCount)
	}

	// full_name should be revoked exactly once (INSERT OR IGNORE).
	var fieldCount int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM revoked_fields WHERE canonical_request_id=? AND field='full_name'`, canonicalID).Scan(&fieldCount)
	if fieldCount != 1 {
		t.Fatalf("expected exactly 1 revoked_fields row for full_name, got %d", fieldCount)
	}
}

// 7. Cross-event-chain: a revocation targets an event in a specific chain and
// only affects that chain; another applicant's chain is untouched.
func TestRevocationDoesNotAffectOtherChain(t *testing.T) {
	srv, _ := newTestServer(t)

	postEvent(t, srv, map[string]any{
		"eventId":      "EV-A-1",
		"channel":      "WEB",
		"submissionId": "W-A",
		"applicant":    map[string]any{"fullName": "Alice", "phone": "+1-555-0700"},
		"consent":      []string{"CONTACT_CALLBACK"},
	})
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":      "EV-B-1",
		"channel":      "WEB",
		"submissionId": "W-B",
		"applicant":    map[string]any{"fullName": "Bob", "phone": "+1-555-0800"},
		"consent":      []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("bob: %d %s", resp.StatusCode, data)
	}
	var bob struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &bob)

	postEvent(t, srv, map[string]any{
		"eventId": "EV-A-REV",
		"revokes": "EV-A-1",
		"scopes":  []string{"CONTACT_CALLBACK"},
	})

	resp, data = get(t, srv, "/v1/intake/canonical/"+bob.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("bob canonical: %d", resp.StatusCode)
	}
	var bobSnap struct {
		Person struct {
			Phone string `json:"phone"`
		} `json:"person"`
		ContactMasked bool `json:"contactMasked"`
	}
	_ = json.Unmarshal(data, &bobSnap)
	if bobSnap.ContactMasked || bobSnap.Person.Phone != "+1-555-0800" {
		t.Fatalf("Bob's chain must be unaffected by Alice's revocation: %s", data)
	}
}

// 8. Audit summary includes revoked fields and the revocation event as
// non-reversible evidence.
func TestAuditIncludesRevokedFields(t *testing.T) {
	srv, _ := newTestServer(t)

	postEvent(t, srv, map[string]any{
		"eventId":      "EV-AUD-1",
		"channel":      "WEB",
		"submissionId": "W-AUD",
		"applicant":    map[string]any{"fullName": "Audit User", "phone": "+1-555-0900", "email": "aud@example.com"},
		"consent":      []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER"},
	})
	resp, data := postEvent(t, srv, map[string]any{
		"eventId": "EV-AUD-REV",
		"revokes": "EV-AUD-1",
		"scopes":  []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER"},
	})
	var cr struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &cr)

	resp, data = get(t, srv, "/v1/intake/canonical/"+cr.ID+"/audit")
	if resp.StatusCode != 200 {
		t.Fatalf("audit: %d %s", resp.StatusCode, data)
	}
	var audit struct {
		EventCount    int      `json:"eventCount"`
		AttemptCount  int      `json:"attemptCount"`
		CurrentConsent []string `json:"currentConsent"`
		RevokedFields []string `json:"revokedFields"`
		Events        []struct {
			EventID string `json:"eventId"`
			Channel string `json:"channel"`
		} `json:"events"`
	}
	_ = json.Unmarshal(data, &audit)
	if audit.EventCount != 2 {
		t.Fatalf("expected 2 events, got %d", audit.EventCount)
	}
	if len(audit.CurrentConsent) != 0 {
		t.Fatalf("expected no consent after both revoked, got %v", audit.CurrentConsent)
	}
	if !containsStr(audit.RevokedFields, "phone") ||
		!containsStr(audit.RevokedFields, "email") ||
		!containsStr(audit.RevokedFields, "full_name") {
		t.Fatalf("expected phone/email/full_name revoked, got %v", audit.RevokedFields)
	}
	foundRev := false
	for _, e := range audit.Events {
		if e.EventID == "EV-AUD-REV" {
			foundRev = true
		}
	}
	if !foundRev {
		t.Fatalf("revocation event must be preserved in audit evidence: %s", data)
	}
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
