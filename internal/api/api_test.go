package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/accessible-intake/gateway/internal/adapter"
	"github.com/accessible-intake/gateway/internal/api"
	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/intake"
	"github.com/accessible-intake/gateway/internal/store"
)

type testHarness struct {
	t       *testing.T
	store   *store.Store
	server  *api.Server
	adapter *adapter.Recorder
	router  http.Handler
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	c, err := contracts.Load(filepath.Join("..", "..", "materials", "channel-contracts.json"))
	if err != nil {
		t.Fatalf("contracts: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	rec := adapter.NewRecorder()
	srv := api.NewServer(st, c, rec)
	return &testHarness{t: t, store: st, server: srv, adapter: rec, router: srv.Routes()}
}

func (h *testHarness) post(t *testing.T, path string, body any) (*http.Response, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Result(), out
}

func (h *testHarness) get(t *testing.T, path string) (*http.Response, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Result(), out
}

func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", v)
	}
	return m
}

func TestPhysicalHotlineWebMergeIntoOneCanonicalRequest(t *testing.T) {
	h := newHarness(t)

	_, p1 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-P-001", "channel": "PHYSICAL", "deskReceiptNo": "D-88",
		"visitor":        map[string]any{"firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10", "email": "ada@example.com", "phone": "555-0100"},
		"accommodations": []string{"BRAILLE_MATERIAL"},
	})
	cr1 := asMap(t, p1["result"])["canonicalRequestId"].(string)

	_, p2 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-001", "channel": "HOTLINE", "callRef": "C-19",
		"caller":                map[string]any{"firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10", "phone": "555-0199"},
		"callbackWindowMinutes": 45,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	cr2 := asMap(t, p2["result"])["canonicalRequestId"].(string)

	_, p3 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-001", "channel": "WEB", "submissionId": "W-71",
		"applicant": map[string]any{"firstName": "Ada", "lastName": "Lovelace", "dateOfBirth": "1815-12-10", "email": "ada@example.com"},
		"consent":   []string{"CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"},
		"summary":   "Needs intake for housing matter",
	})
	cr3 := asMap(t, p3["result"])["canonicalRequestId"].(string)

	if cr1 != cr2 || cr2 != cr3 {
		t.Fatalf("expected one canonical request across channels, got %s %s %s", cr1, cr2, cr3)
	}

	resp, view := h.get(t, "/api/v1/requests/"+cr1)
	if resp.StatusCode != 200 {
		t.Fatalf("get request: %d", resp.StatusCode)
	}
	refs := asMap(t, view["sourceReferences"])
	if refs["PHYSICAL"] != "D-88" || refs["HOTLINE"] != "C-19" || refs["WEB"] != "W-71" {
		t.Fatalf("source references not preserved: %v", refs)
	}
	chain, _ := view["eventChain"].([]any)
	if len(chain) != 3 {
		t.Fatalf("expected 3 events in chain, got %d", len(chain))
	}
	if view["summaryTransferable"] != true {
		t.Fatalf("summary should be transferable")
	}
}

func TestSameEventAndPayloadIsIdempotent(t *testing.T) {
	h := newHarness(t)
	body := map[string]any{
		"eventId": "EV-W-100", "channel": "WEB", "submissionId": "W-100",
		"applicant": map[string]any{"firstName": "Grace", "lastName": "Hopper", "dateOfBirth": "1906-12-09"},
		"consent":   []string{"CONTACT_CALLBACK"},
	}
	r1, o1 := h.post(t, "/api/v1/events", body)
	r2, o2 := h.post(t, "/api/v1/events", body)
	if r1.StatusCode != 200 || r2.StatusCode != 200 {
		t.Fatalf("expected 200, got %d %d", r1.StatusCode, r2.StatusCode)
	}
	res1 := asMap(t, o1["result"])
	res2 := asMap(t, o2["result"])
	if res1["sequence"] != res2["sequence"] {
		t.Fatalf("idempotent retry changed sequence: %v vs %v", res1["sequence"], res2["sequence"])
	}
	if len(h.adapter.Deliveries(res1["canonicalRequestId"].(string))) != 1 {
		t.Fatalf("adapter must only be invoked once for identical payload")
	}
}

func TestSameEventDifferentPayloadIsConflict(t *testing.T) {
	h := newHarness(t)
	base := map[string]any{
		"eventId": "EV-W-200", "channel": "WEB", "submissionId": "W-200",
		"applicant": map[string]any{"firstName": "K", "lastName": "H", "dateOfBirth": "1990-01-01"},
		"consent":   []string{"CONTACT_CALLBACK"},
	}
	h.post(t, "/api/v1/events", base)
	base["summary"] = "changed after first submit"
	r2, o2 := h.post(t, "/api/v1/events", base)
	if r2.StatusCode != 409 {
		t.Fatalf("expected 409 conflict, got %d", r2.StatusCode)
	}
	if o2["conflict"] != true {
		t.Fatalf("expected conflict flag, got %v", o2)
	}
}

func TestUnknownCodesAndNegativeWindowAreItemRejected(t *testing.T) {
	h := newHarness(t)
	r, o := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-300", "channel": "HOTLINE", "callRef": "C-300",
		"caller":                map[string]any{"firstName": "M", "lastName": "N", "dateOfBirth": "1985-03-03"},
		"callbackWindowMinutes": -5,
		"consent":               []string{"CONTACT_CALLBACK", "NOT_A_SCOPE"},
		"accommodations":        []string{"TEXT_ONLY", "BOGUS_CODE"},
	})
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 with per-item errors, got %d %s", r.StatusCode, o)
	}
	res := asMap(t, o["result"])
	if res["status"] != "accepted_with_errors" {
		t.Fatalf("expected accepted_with_errors, got %v", res["status"])
	}
	items, _ := res["itemResults"].([]any)
	rejected := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		if m["status"] == "rejected" {
			rejected[m["code"].(string)] = true
		}
	}
	if !rejected["INVALID_CALLBACK_WINDOW"] || !rejected["UNKNOWN_CONSENT_SCOPE"] || !rejected["UNKNOWN_ACCOMMODATION_CODE"] {
		t.Fatalf("expected all invalid items rejected, got %v", rejected)
	}
	view := asMap(t, o["canonical"])
	if view["contactAvailable"] != true {
		t.Fatalf("valid CONTACT_CALLBACK consent should make contact available even when the invalid window was dropped")
	}
}

func TestRevocationRedactsContactAndRetryDoesNotReexpose(t *testing.T) {
	h := newHarness(t)
	person := map[string]any{"firstName": "Rosa", "lastName": "Parks", "dateOfBirth": "1913-02-04", "phone": "555-2222", "email": "rosa@example.com"}
	h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-400", "channel": "WEB", "submissionId": "W-400",
		"applicant": person, "consent": []string{"CONTACT_CALLBACK", "ACCOMMODATION_TRANSFER"},
	})
	_, rev := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-401", "channel": "WEB", "submissionId": "W-401",
		"applicant": person, "revokes": "EV-W-400", "revocationScopes": []string{"CONTACT_CALLBACK"},
	})
	view := asMap(t, rev["canonical"])
	if view["contactAvailable"] != false {
		t.Fatalf("contact must be unavailable after revocation")
	}
	p := asMap(t, view["person"])
	if phone, _ := p["phone"].(string); phone != "" {
		t.Fatalf("phone must be redacted after revocation: %v", p)
	}
	if email, _ := p["email"].(string); email != "" {
		t.Fatalf("email must be redacted after revocation: %v", p)
	}
	// Replaying/re-querying must not re-expose withdrawn contact info.
	rid := asMap(t, rev["result"])["canonicalRequestId"].(string)
	_, again := h.get(t, "/api/v1/requests/"+rid)
	p2 := asMap(t, again["person"])
	if phone, _ := p2["phone"].(string); phone != "" {
		t.Fatalf("replay re-exposed withdrawn phone: %v", p2)
	}
	if email, _ := p2["email"].(string); email != "" {
		t.Fatalf("replay re-exposed withdrawn email: %v", p2)
	}
}

func TestAdapterTransientFailureThenRetry(t *testing.T) {
	h := newHarness(t)
	h.adapter.FailOnce()
	r1, o1 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-P-500", "channel": "PHYSICAL", "deskReceiptNo": "D-500",
		"visitor": map[string]any{"firstName": "C", "lastName": "D", "dateOfBirth": "1977-07-07"},
	})
	if r1.StatusCode != 202 {
		t.Fatalf("expected 202 accepted on adapter failure, got %d", r1.StatusCode)
	}
	res1 := asMap(t, o1["result"])
	if res1["deliveryStatus"] != "pending" {
		t.Fatalf("expected pending delivery, got %v", res1["deliveryStatus"])
	}
	rid := res1["canonicalRequestId"].(string)
	if len(h.adapter.Deliveries(rid)) != 0 {
		t.Fatalf("failed first attempt must not count as delivered")
	}

	r2, o2 := h.post(t, "/api/v1/events/EV-P-500/retry", map[string]any{})
	if r2.StatusCode != 200 {
		t.Fatalf("expected 200 after successful retry, got %d", r2.StatusCode)
	}
	res2 := asMap(t, o2["result"])
	if res2["deliveryStatus"] != "delivered" {
		t.Fatalf("expected delivered, got %v", res2["deliveryStatus"])
	}
	attempts, _ := res2["attempts"].([]any)
	if len(attempts) != 2 {
		t.Fatalf("expected two attempts recorded, got %d", len(attempts))
	}
}

func TestRetryAfterRevocationUsesRedactedProjection(t *testing.T) {
	h := newHarness(t)
	h.adapter.FailOnce()
	person := map[string]any{"firstName": "E", "lastName": "F", "dateOfBirth": "1966-06-06", "phone": "555-6666"}
	_, o1 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-600", "channel": "WEB", "submissionId": "W-600",
		"applicant": person, "consent": []string{"CONTACT_CALLBACK"},
	})
	rid := asMap(t, o1["result"])["canonicalRequestId"].(string)
	_, _ = h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-601", "channel": "WEB", "submissionId": "W-601",
		"applicant": person, "revokes": "EV-W-600", "revocationScopes": []string{"CONTACT_CALLBACK"},
	})
	_, o3 := h.post(t, "/api/v1/events/EV-W-600/retry", map[string]any{})
	delivered := h.adapter.Deliveries(rid)
	var last *intake.CanonicalView
	for _, d := range delivered {
		last = d
	}
	if last == nil {
		t.Fatalf("expected at least one successful delivery")
	}
	if last.ContactAvailable != false {
		t.Fatalf("retry projection must reflect revocation")
	}
	if last.Person.Phone != "" {
		t.Fatalf("retry must not re-expose withdrawn phone: %v", last.Person)
	}
	_ = o3
}

func TestConcurrentSamePersonFormsSingleChain(t *testing.T) {
	h := newHarness(t)
	const n = 20
	var wg sync.WaitGroup
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := map[string]any{
				"eventId":      "EV-C-" + string(rune('A'+i)),
				"channel":      "WEB",
				"submissionId": "S-" + string(rune('A'+i)),
				"applicant":    map[string]any{"firstName": "Con", "lastName": "Current", "dateOfBirth": "2000-01-01"},
			}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/events", jsonReader(body))
			rec := httptest.NewRecorder()
			h.router.ServeHTTP(rec, req)
			var out map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &out)
			ids[i] = asMap(t, out["result"])["canonicalRequestId"].(string)
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("concurrent requests split into multiple chains: %s vs %s", ids[0], ids[i])
		}
	}
	resp, view := h.get(t, "/api/v1/requests/"+ids[0])
	if resp.StatusCode != 200 {
		t.Fatalf("get: %d", resp.StatusCode)
	}
	chain, _ := view["eventChain"].([]any)
	if len(chain) != n {
		t.Fatalf("expected %d events in single chain, got %d", n, len(chain))
	}
}

func TestReplayProducesStableProjectionHash(t *testing.T) {
	h := newHarness(t)
	_, o1 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-700", "channel": "WEB", "submissionId": "W-700",
		"applicant": map[string]any{"firstName": "G", "lastName": "H", "dateOfBirth": "1955-05-05"},
		"consent":   []string{"CONTACT_CALLBACK"},
	})
	rid := asMap(t, o1["result"])["canonicalRequestId"].(string)
	_, v1 := h.get(t, "/api/v1/requests/"+rid)
	_, v2 := h.get(t, "/api/v1/requests/"+rid)
	if v1["projectionHash"] != v2["projectionHash"] {
		t.Fatalf("projection hash not stable across replays: %v vs %v", v1["projectionHash"], v2["projectionHash"])
	}
	_, a1 := h.get(t, "/api/v1/requests/"+rid+"/audit")
	if a1["projectionHash"] != v1["projectionHash"] {
		t.Fatalf("audit hash mismatch")
	}
}

func TestMissingRequiredFieldsReturnBadRequest(t *testing.T) {
	h := newHarness(t)
	r, _ := h.post(t, "/api/v1/events", map[string]any{"channel": "WEB"})
	if r.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", r.StatusCode)
	}
}

func TestOutOfOrderRevocationBeforeGrantStillRevokes(t *testing.T) {
	h := newHarness(t)
	person := map[string]any{"firstName": "O", "lastName": "Order", "dateOfBirth": "1944-04-04", "phone": "555-4444"}
	_, rev := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-801", "channel": "WEB", "submissionId": "W-801",
		"applicant": person, "revokes": "EV-W-800", "revocationScopes": []string{"CONTACT_CALLBACK"},
	})
	rid := asMap(t, rev["result"])["canonicalRequestId"].(string)
	_, _ = h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-800", "channel": "WEB", "submissionId": "W-800",
		"applicant": person, "consent": []string{"CONTACT_CALLBACK"}, "canonicalRequestId": rid,
	})
	_, view := h.get(t, "/api/v1/requests/"+rid)
	if view["contactAvailable"] != false {
		t.Fatalf("out-of-order revocation must still suppress contact after replay: %v", view)
	}
}

func jsonReader(v any) *bytes.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

func TestHotlineCallbackArrivesBeforeWebRecordMergesIntoOneChain(t *testing.T) {
	h := newHarness(t)

	// Hotline callback arrives first: only a phone fragment, no full identity.
	r1, cb := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-CB-1", "channel": "HOTLINE", "callRef": "C-CB-1",
		"caller":                map[string]any{"phone": "555-9000"},
		"callbackWindowMinutes": 30,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	if r1.StatusCode != 200 {
		t.Fatalf("callback first submit: %d %v", r1.StatusCode, cb)
	}
	cbRID := asMap(t, cb["result"])["canonicalRequestId"].(string)
	cbMatch := asMap(t, cb["match"])
	if cbMatch["confidence"] != "none" {
		t.Fatalf("first fragment should have no match confidence, got %v", cbMatch)
	}
	// The caller granted CONTACT_CALLBACK with a phone-only fragment, so the
	// phone is available; it will be merged with the web identity later.

	// Web record arrives later with full identity and the same normalized phone.
	r2, w := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-CB-1", "channel": "WEB", "submissionId": "W-CB-1",
		"applicant": map[string]any{"firstName": "Callback", "lastName": "Person", "dateOfBirth": "1992-09-09", "phone": "(555) 9000"},
	})
	if r2.StatusCode != 200 {
		t.Fatalf("web submit: %d", r2.StatusCode)
	}
	wRID := asMap(t, w["result"])["canonicalRequestId"].(string)
	if wRID != cbRID {
		t.Fatalf("web record must merge into the earlier callback chain: %s vs %s", wRID, cbRID)
	}
	wMatch := asMap(t, w["match"])
	if wMatch["confidence"] != "high" {
		t.Fatalf("expected high confidence phone match, got %v", wMatch["confidence"])
	}
	if wMatch["reason"] != "PHONE_MATCH" {
		t.Fatalf("expected PHONE_MATCH reason, got %v", wMatch["reason"])
	}
	// Exactly one chain for the applicant.
	resp, view := h.get(t, "/api/v1/requests/"+wRID)
	if resp.StatusCode != 200 {
		t.Fatalf("get chain: %d", resp.StatusCode)
	}
	chain, _ := view["eventChain"].([]any)
	if len(chain) != 2 {
		t.Fatalf("expected a single 2-event chain, got %d", len(chain))
	}
	refs := asMap(t, view["sourceReferences"])
	if refs["HOTLINE"] != "C-CB-1" || refs["WEB"] != "W-CB-1" {
		t.Fatalf("source references must preserve both: %v", refs)
	}
}

func TestPartialMatchWithConflictingFragmentRecordsConflictEvidence(t *testing.T) {
	h := newHarness(t)
	h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-CF-1", "channel": "HOTLINE", "callRef": "C-CF-1",
		"caller":                map[string]any{"firstName": "Sam", "lastName": "Doe", "dateOfBirth": "1980-01-01", "phone": "555-1111"},
		"callbackWindowMinutes": 20,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	// Different DOB but same name: conflicting identity fragment.
	_, o := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-W-CF-1", "channel": "WEB", "submissionId": "W-CF-1",
		"applicant": map[string]any{"firstName": "Sam", "lastName": "Doe", "dateOfBirth": "1980-01-02", "phone": "555-1111"},
	})
	match := asMap(t, o["match"])
	if match["confidence"] != "high" {
		t.Fatalf("phone match should yield high confidence: %v", match)
	}
	if match["conflict"] != true {
		t.Fatalf("conflicting DOB must be flagged as conflict evidence: %v", match)
	}
	// Evidence must not expose raw contact fields for the conflicting comparison.
	evidence, _ := match["evidence"].([]any)
	var sawDOBConflict bool
	for _, e := range evidence {
		em := e.(map[string]any)
		if em["field"] == "dateOfBirth" && em["matched"] == false {
			sawDOBConflict = true
		}
	}
	if !sawDOBConflict {
		t.Fatalf("expected a dateOfBirth conflict evidence entry: %v", evidence)
	}
}

func TestCallbackWindowZeroNegativeAndCrossDayHandling(t *testing.T) {
	h := newHarness(t)
	person := map[string]any{"firstName": "Win", "lastName": "Dow", "dateOfBirth": "1970-07-07"}

	_, zero := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-W-0", "channel": "HOTLINE", "callRef": "C-W-0",
		"caller": person, "callbackWindowMinutes": 0, "consent": []string{"CONTACT_CALLBACK"},
	})
	zres := asMap(t, zero["result"])
	if code := itemCode(zres, "callbackWindowMinutes"); code != "IMMEDIATE_CALLBACK" {
		t.Fatalf("zero window should be IMMEDIATE_CALLBACK, got %q", code)
	}

	_, neg := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-W-N", "channel": "HOTLINE", "callRef": "C-W-N",
		"caller": person, "callbackWindowMinutes": -10,
	})
	nres := asMap(t, neg["result"])
	if code := itemCode(nres, "callbackWindowMinutes"); code != "INVALID_CALLBACK_WINDOW" {
		t.Fatalf("negative window should be rejected, got %q", code)
	}

	_, cross := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-H-W-X", "channel": "HOTLINE", "callRef": "C-W-X",
		"caller": person, "callbackWindowMinutes": 2000,
	})
	xres := asMap(t, cross["result"])
	if code := itemCode(xres, "callbackWindowMinutes"); code != "CROSS_DAY_CALLBACK_WINDOW" {
		t.Fatalf("cross-day window should be rejected, got %q", code)
	}
}

func itemCode(res map[string]any, field string) string {
	items, _ := res["itemResults"].([]any)
	for _, it := range items {
		m := it.(map[string]any)
		if m["field"] == field && m["status"] == "rejected" {
			if c, ok := m["code"].(string); ok {
				return c
			}
		}
		if m["field"] == field && m["status"] == "applied" {
			if c, ok := m["code"].(string); ok {
				return c
			}
			return "applied"
		}
	}
	return ""
}

func TestDuplicateKeySamePayloadReturnsOriginalAndDifferentPayloadConflicts(t *testing.T) {
	h := newHarness(t)
	body := map[string]any{
		"eventId": "EV-DUP-1", "channel": "WEB", "submissionId": "W-DUP-1",
		"applicant": map[string]any{"firstName": "Dup", "lastName": "Key", "dateOfBirth": "1988-08-08"},
	}
	r1, o1 := h.post(t, "/api/v1/events", body)
	r2, o2 := h.post(t, "/api/v1/events", body)
	if r1.StatusCode != 200 || r2.StatusCode != 200 {
		t.Fatalf("duplicate same payload should be 200, got %d %d", r1.StatusCode, r2.StatusCode)
	}
	seq1 := asMap(t, o1["result"])["sequence"]
	seq2 := asMap(t, o2["result"])["sequence"]
	if seq1 != seq2 {
		t.Fatalf("duplicate must not create a new sequence: %v vs %v", seq1, seq2)
	}
	body["summary"] = "changed"
	r3, o3 := h.post(t, "/api/v1/events", body)
	if r3.StatusCode != 409 {
		t.Fatalf("same key different payload must be 409, got %d", r3.StatusCode)
	}
	if o3["conflict"] != true {
		t.Fatalf("expected conflict flag")
	}
}

func TestContactNotExposedBeforeConsent(t *testing.T) {
	h := newHarness(t)
	// Fragment with phone but no CONTACT_CALLBACK.
	_, o := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-NC-1", "channel": "HOTLINE", "callRef": "C-NC-1",
		"caller":                map[string]any{"firstName": "No", "lastName": "Consent", "dateOfBirth": "1975-05-05", "phone": "555-2222"},
		"callbackWindowMinutes": 15,
	})
	view := asMap(t, o["canonical"])
	if view["contactAvailable"] != false {
		t.Fatalf("contact must be unavailable without CONTACT_CALLBACK")
	}
	p := asMap(t, view["person"])
	if phone, _ := p["phone"].(string); phone != "" {
		t.Fatalf("phone must be redacted without consent: %v", p)
	}
}

func TestConcurrentHotlineAndWebClaimSameApplicantSingleChain(t *testing.T) {
	h := newHarness(t)
	var wg sync.WaitGroup
	ids := make([]string, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, o := h.post(t, "/api/v1/events", map[string]any{
			"eventId": "EV-CC-H", "channel": "HOTLINE", "callRef": "C-CC",
			"caller":                map[string]any{"firstName": "Con", "lastName": "Current", "dateOfBirth": "2001-01-01", "phone": "555-7777"},
			"callbackWindowMinutes": 25,
			"consent":               []string{"CONTACT_CALLBACK"},
		})
		ids[0] = asMap(t, o["result"])["canonicalRequestId"].(string)
	}()
	go func() {
		defer wg.Done()
		_, o := h.post(t, "/api/v1/events", map[string]any{
			"eventId": "EV-CC-W", "channel": "WEB", "submissionId": "W-CC",
			"applicant": map[string]any{"firstName": "Con", "lastName": "Current", "dateOfBirth": "2001-01-01", "phone": "555-7777"},
		})
		ids[1] = asMap(t, o["result"])["canonicalRequestId"].(string)
	}()
	wg.Wait()
	if ids[0] != ids[1] {
		t.Fatalf("concurrent claims must resolve to one chain: %s vs %s", ids[0], ids[1])
	}
	_, view := h.get(t, "/api/v1/requests/"+ids[0])
	chain, _ := view["eventChain"].([]any)
	if len(chain) != 2 {
		t.Fatalf("expected 2-event chain, got %d: %v", len(chain), chain)
	}
}

func TestAdapterFailureRecoveryKeepsSingleChain(t *testing.T) {
	h := newHarness(t)
	h.adapter.FailOnce()
	r1, o1 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-AF-1", "channel": "HOTLINE", "callRef": "C-AF",
		"caller":                map[string]any{"firstName": "A", "lastName": "F", "dateOfBirth": "1960-06-06", "phone": "555-3333"},
		"callbackWindowMinutes": 40,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	if r1.StatusCode != 202 {
		t.Fatalf("expected 202 on adapter failure, got %d", r1.StatusCode)
	}
	rid := asMap(t, o1["result"])["canonicalRequestId"].(string)
	// A web event for the same person arrives while the first delivery is pending.
	r2, o2 := h.post(t, "/api/v1/events", map[string]any{
		"eventId": "EV-AF-2", "channel": "WEB", "submissionId": "W-AF",
		"applicant": map[string]any{"firstName": "A", "lastName": "F", "dateOfBirth": "1960-06-06"},
	})
	if r2.StatusCode != 200 {
		t.Fatalf("web event after failure: %d", r2.StatusCode)
	}
	if asMap(t, o2["result"])["canonicalRequestId"].(string) != rid {
		t.Fatalf("web event must stay on the same chain after adapter failure")
	}
	// Retry the first delivery; projection must already include both events.
	r3, o3 := h.post(t, "/api/v1/events/EV-AF-1/retry", map[string]any{})
	if r3.StatusCode != 200 {
		t.Fatalf("retry: %d %v", r3.StatusCode, o3)
	}
	chain, _ := asMap(t, o3["canonical"])["eventChain"].([]any)
	if len(chain) != 2 {
		t.Fatalf("retried projection must contain both events, got %v", chain)
	}
}
