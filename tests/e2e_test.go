package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/httpapi"
	"github.com/accessible-intake/gateway/internal/intake"
	"github.com/accessible-intake/gateway/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	c := contracts.MustLoad()
	svc := intake.New(st, c)
	srv := httptest.NewServer(httpapi.New(svc).Handler())
	t.Cleanup(srv.Close)
	return srv, st
}

func postEvent(t *testing.T, srv *httptest.Server, body any) (*http.Response, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(srv.URL+"/v1/intake/events", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("post event: %v", err)
	}
	data, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, data
}

func get(t *testing.T, srv *httptest.Server, path string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	data, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, data
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// 1. The three fixture examples, when they belong to the same applicant, are
// merged into a single canonical request chain.
func TestThreeChannelsFormOneCanonicalChain(t *testing.T) {
	srv, _ := newTestServer(t)

	// PHYSICAL: deskReceiptNo / visitor / accommodations
	resp, data := postEvent(t, srv, map[string]any{
		"eventId":       "EV-P-001",
		"channel":       "PHYSICAL",
		"deskReceiptNo": "D-88",
		"visitor":       map[string]any{"fullName": "Jane Doe", "phone": "+1-555-0100", "email": "jane@example.com"},
		"accommodations": []string{"BRAILLE_MATERIAL"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("physical: status %d body %s", resp.StatusCode, data)
	}
	var physical struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &physical)

	// HOTLINE: callRef / caller / callbackWindowMinutes / consent
	resp, data = postEvent(t, srv, map[string]any{
		"eventId":               "EV-H-001",
		"channel":               "HOTLINE",
		"callRef":               "C-19",
		"caller":                map[string]any{"fullName": "Jane Doe", "phone": "+1-555-0100"},
		"callbackWindowMinutes": 45,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("hotline: status %d body %s", resp.StatusCode, data)
	}
	var hotline struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &hotline)

	// WEB: submissionId / applicant / consent
	resp, data = postEvent(t, srv, map[string]any{
		"eventId":      "EV-W-001",
		"channel":      "WEB",
		"submissionId": "W-71",
		"applicant":    map[string]any{"fullName": "Jane Doe", "email": "jane@example.com"},
		"consent":      []string{"CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("web: status %d body %s", resp.StatusCode, data)
	}
	var web struct {
		CanonicalRequestID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &web)

	if physical.CanonicalRequestID != hotline.CanonicalRequestID ||
		hotline.CanonicalRequestID != web.CanonicalRequestID {
		t.Fatalf("expected one canonical id, got %s %s %s",
			physical.CanonicalRequestID, hotline.CanonicalRequestID, web.CanonicalRequestID)
	}

	// Canonical snapshot aggregates all three sources, accommodations, consent.
	resp, data = get(t, srv, "/v1/intake/canonical/"+web.CanonicalRequestID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d %s", resp.StatusCode, data)
	}
	var snap map[string]any
	_ = json.Unmarshal(data, &snap)
	if snap["contactMasked"] != false {
		t.Fatalf("contact should not be masked with CONTACT_CALLBACK granted: %s", data)
	}
	sources, _ := snap["sources"].([]any)
	if len(sources) != 3 {
		t.Fatalf("expected 3 sources, got %d: %s", len(sources), data)
	}
	acc, _ := snap["accommodations"].([]any)
	if len(acc) != 1 || acc[0] != "BRAILLE_MATERIAL" {
		t.Fatalf("expected BRAILLE_MATERIAL accommodation: %s", data)
	}
	consent, _ := snap["consent"].([]any)
	if len(consent) != 3 {
		t.Fatalf("expected 3 consent scopes, got %v", consent)
	}
	phone, _ := snap["person"].(map[string]any)["phone"].(string)
	if phone != "+1-555-0100" {
		t.Fatalf("expected phone to be exposed, got %q", phone)
	}
}

// 2. Same eventId + same payload returns the original result (idempotent).
func TestIdempotencySamePayloadReturnsOriginal(t *testing.T) {
	srv, _ := newTestServer(t)
	body := map[string]any{
		"eventId":      "EV-IDEM-1",
		"channel":      "WEB",
		"submissionId": "W-100",
		"applicant":    map[string]any{"fullName": "Pat Lee", "email": "pat@example.com"},
		"consent":      []string{"CASE_SUMMARY_TRANSFER"},
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
		ID              string `json:"canonicalRequestId"`
		IdempotentReplay bool   `json:"idempotentReplay"`
	}
	_ = json.Unmarshal(data1, &r1)
	_ = json.Unmarshal(data2, &r2)
	if r1.ID != r2.ID {
		t.Fatalf("canonical id changed: %s vs %s", r1.ID, r2.ID)
	}
	if !r2.IdempotentReplay {
		t.Fatalf("expected idempotentReplay=true on retry, got %s", data2)
	}
}

// 3. Same eventId with a different payload is a 409 conflict.
func TestConflictOnSameEventIDDifferentPayload(t *testing.T) {
	srv, _ := newTestServer(t)
	base := map[string]any{
		"eventId":      "EV-CONF-1",
		"channel":      "WEB",
		"submissionId": "W-200",
		"applicant":    map[string]any{"fullName": "Sam Ray", "email": "sam@example.com"},
	}
	if resp, data := postEvent(t, srv, base); resp.StatusCode != 200 {
		t.Fatalf("first: %d %s", resp.StatusCode, data)
	}
	changed := map[string]any{
		"eventId":      "EV-CONF-1",
		"channel":      "WEB",
		"submissionId": "W-200",
		"applicant":    map[string]any{"fullName": "Sam Ray", "email": "sam.ray@example.com"},
	}
	resp, data := postEvent(t, srv, changed)
	if resp.StatusCode != 409 {
		t.Fatalf("expected 409, got %d: %s", resp.StatusCode, data)
	}
	var result map[string]any
	_ = json.Unmarshal(data, &result)
	if result["status"] != "CONFLICT" {
		t.Fatalf("expected status CONFLICT, got %s", data)
	}
	if result["conflict"] == nil {
		t.Fatalf("expected conflict details, got %s", data)
	}
}

// 4. Per-item errors do not abort valid items.
func TestPerItemValidationErrors(t *testing.T) {
	srv, _ := newTestServer(t)
	neg := -5
	body := map[string]any{
		"eventId":               "EV-INVALID-1",
		"channel":               "HOTLINE",
		"callRef":               "C-BAD",
		"caller":                map[string]any{"fullName": "Casey Ng", "phone": "+1-555-0101"},
		"callbackWindowMinutes": neg,
		"accommodations":        []string{"BRAILLE_MATERIAL", "NOPE_CODE"},
		"consent":               []string{"CONTACT_CALLBACK", "BOGUS_SCOPE"},
	}
	resp, data := postEvent(t, srv, body)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 (partial), got %d: %s", resp.StatusCode, data)
	}
	var r struct {
		Status      string `json:"status"`
		ItemResults []struct {
			Item string `json:"item"`
			Code string `json:"code"`
		} `json:"itemResults"`
		Canonical struct {
			Accommodations []string `json:"accommodations"`
			Consent        []string `json:"consent"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data, &r)
	if r.Status != "ACCEPTED" {
		t.Fatalf("expected ACCEPTED, got %s", r.Status)
	}
	codes := map[string]bool{}
	for _, ir := range r.ItemResults {
		codes[ir.Code] = true
	}
	for _, want := range []string{"UNKNOWN_CODE", "UNKNOWN_SCOPE", "INVALID_VALUE"} {
		if !codes[want] {
			t.Fatalf("expected per-item code %q in %v", want, r.ItemResults)
		}
	}
	// Valid items still committed.
	if len(r.Canonical.Accommodations) != 1 || r.Canonical.Accommodations[0] != "BRAILLE_MATERIAL" {
		t.Fatalf("valid accommodation not committed: %v", r.Canonical.Accommodations)
	}
	if len(r.Canonical.Consent) != 1 || r.Canonical.Consent[0] != "CONTACT_CALLBACK" {
		t.Fatalf("valid consent not committed: %v", r.Canonical.Consent)
	}
}

// 5. Fatal top-level errors reject the whole submission.
func TestFatalValidation(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, data := postEvent(t, srv, map[string]any{"channel": "WEB"})
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, data)
	}
	resp, data = postEvent(t, srv, map[string]any{"eventId": "X", "channel": "FAX"})
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for unknown channel, got %d: %s", resp.StatusCode, data)
	}
}

// 6. Revocation masks contact info; a retry of the original event must not
// re-expose it.
func TestRevocationMasksContactAndRetryDoesNotReveal(t *testing.T) {
	srv, _ := newTestServer(t)
	original := map[string]any{
		"eventId":      "EV-W-001",
		"channel":      "WEB",
		"submissionId": "W-71",
		"applicant":    map[string]any{"fullName": "Jane Doe", "phone": "+1-555-0100", "email": "jane@example.com"},
		"consent":      []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER"},
	}
	resp, data := postEvent(t, srv, original)
	if resp.StatusCode != 200 {
		t.Fatalf("original: %d %s", resp.StatusCode, data)
	}
	var r struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data, &r)

	// Revoke CONTACT_CALLBACK using the fixture revocation shape.
	resp, data = postEvent(t, srv, map[string]any{
		"eventId": "EV-W-002",
		"revokes": "EV-W-001",
		"scopes":  []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("revocation: %d %s", resp.StatusCode, data)
	}

	resp, data = get(t, srv, "/v1/intake/canonical/"+r.ID)
	if resp.StatusCode != 200 {
		t.Fatalf("canonical: %d %s", resp.StatusCode, data)
	}
	var snap struct {
		ContactMasked bool `json:"contactMasked"`
		Person        struct {
			Phone string `json:"phone"`
			Email string `json:"email"`
		} `json:"person"`
	}
	_ = json.Unmarshal(data, &snap)
	if !snap.ContactMasked {
		t.Fatalf("expected contactMasked=true after revocation: %s", data)
	}
	if snap.Person.Phone != "" || snap.Person.Email != "" {
		t.Fatalf("contact must be redacted after revocation, got phone=%q email=%q", snap.Person.Phone, snap.Person.Email)
	}

	// Retry the original event idempotently: contact must stay masked.
	resp, data = postEvent(t, srv, original)
	if resp.StatusCode != 200 {
		t.Fatalf("retry: %d %s", resp.StatusCode, data)
	}
	var retry struct {
		IdempotentReplay bool `json:"idempotentReplay"`
		Canonical        struct {
			ContactMasked bool   `json:"contactMasked"`
			Phone         string `json:"phone"`
			Email         string `json:"email"`
		} `json:"canonical"`
	}
	_ = json.Unmarshal(data, &retry)
	if !retry.IdempotentReplay {
		t.Fatalf("retry should be idempotent: %s", data)
	}
	if !retry.Canonical.ContactMasked || retry.Canonical.Phone != "" || retry.Canonical.Email != "" {
		t.Fatalf("retry must not re-expose withdrawn contact: %s", data)
	}
}

// 7. Out-of-order revocation (target missing) returns a clear per-event error.
func TestOutOfOrderRevocationTargetMissing(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, data := postEvent(t, srv, map[string]any{
		"eventId": "EV-REV-EARLY",
		"revokes": "EV-NOT-YET",
		"scopes":  []string{"CONTACT_CALLBACK"},
	})
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, data)
	}
	if !bytes.Contains(data, []byte("REVOCATION_TARGET_MISSING")) {
		t.Fatalf("expected REVOCATION_TARGET_MISSING, got %s", data)
	}
}

// 8. Two channels concurrently reporting the same subject form exactly one chain.
func TestConcurrentSameSubjectFormsOneChain(t *testing.T) {
	srv, _ := newTestServer(t)
	mk := func(id, source, name string) map[string]any {
		return map[string]any{
			"eventId":      id,
			"channel":      "WEB",
			"submissionId": source,
			"applicant":    map[string]any{"fullName": name, "email": "dual@example.com"},
		}
	}
	const n = 8
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, data := postEvent(t, srv, mk(fmt.Sprintf("EV-CONC-%d", i), fmt.Sprintf("W-%d", i), "Dual User"))
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
			t.Fatalf("expected all events in one chain, got %q vs %q", first, ids[i])
		}
	}
	// Audit shows all events in one chain with contiguous sequence.
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
			t.Fatalf("expected contiguous sequence, got %v", audit.Events)
		}
	}
}

// 9. Concurrent submissions of the SAME eventId + payload commit once.
func TestConcurrentSameEventIdempotent(t *testing.T) {
	srv, st := newTestServer(t)
	body := map[string]any{
		"eventId":      "EV-RACE",
		"channel":      "WEB",
		"submissionId": "W-RACE",
		"applicant":    map[string]any{"fullName": "Race User", "email": "race@example.com"},
	}
	const n = 6
	var wg sync.WaitGroup
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, _ := postEvent(t, srv, body)
			statuses[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()
	for i, s := range statuses {
		if s != 200 {
			t.Fatalf("request %d: expected 200, got %d", i, s)
		}
	}
	// Exactly one event row committed.
	var count int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-RACE'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 committed event, got %d", count)
	}
}

// 10. Simulated adapter transient failure then a successful retry.
func TestAdapterTransientThenRetry(t *testing.T) {
	srv, st := newTestServer(t)
	body := map[string]any{
		"eventId":                  "EV-TRANSIENT",
		"channel":                  "PHYSICAL",
		"deskReceiptNo":            "D-TR",
		"visitor":                  map[string]any{"fullName": "Temporary User", "phone": "+1-555-0199"},
		"simulateAdapterTransient": true,
	}
	resp, data := postEvent(t, srv, body)
	if resp.StatusCode != 503 {
		t.Fatalf("expected 503, got %d: %s", resp.StatusCode, data)
	}
	// Event was NOT committed.
	var count int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-TRANSIENT'`).Scan(&count)
	if count != 0 {
		t.Fatalf("event must not be committed on transient failure, got %d", count)
	}

	// Retry without the transient flag.
	body["simulateAdapterTransient"] = false
	resp, data = postEvent(t, srv, body)
	if resp.StatusCode != 200 {
		t.Fatalf("retry: expected 200, got %d: %s", resp.StatusCode, data)
	}
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM events WHERE event_id='EV-TRANSIENT'`).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 event after retry, got %d", count)
	}
	// Two attempts recorded: one ADAPTER_TRANSIENT, one OK.
	resp, data = get(t, srv, "/v1/intake/events/EV-TRANSIENT/attempts")
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
		t.Fatalf("expected ADAPTER_TRANSIENT and OK attempts, got %s", data)
	}
}

// 11. Replay reconstructs the same final normalized result.
func TestReplayMatchesSnapshot(t *testing.T) {
	srv, _ := newTestServer(t)
	postEvent(t, srv, map[string]any{
		"eventId": "EV-R1", "channel": "PHYSICAL", "deskReceiptNo": "D-R1",
		"visitor":        map[string]any{"fullName": "Replay User", "email": "replay@example.com"},
		"accommodations": []string{"SIGN_INTERPRETER", "STEP_FREE_ACCESS"},
	})
	postEvent(t, srv, map[string]any{
		"eventId": "EV-R2", "channel": "HOTLINE", "callRef": "C-R1",
		"caller":                map[string]any{"fullName": "Replay User", "phone": "+1-555-0150"},
		"callbackWindowMinutes": 30,
		"consent":               []string{"CONTACT_CALLBACK"},
	})
	// Derive canonical id from the third event response.
	_, data2 := postEvent(t, srv, map[string]any{
		"eventId": "EV-R3", "channel": "WEB", "submissionId": "W-R1",
		"applicant": map[string]any{"fullName": "Replay User", "email": "replay@example.com"},
	})
	var r3 struct {
		ID string `json:"canonicalRequestId"`
	}
	_ = json.Unmarshal(data2, &r3)

	snapshotResp, snapData := get(t, srv, "/v1/intake/canonical/"+r3.ID)
	if snapshotResp.StatusCode != 200 {
		t.Fatalf("snapshot: %d %s", snapshotResp.StatusCode, snapData)
	}
	replayResp, replayData := postReplay(t, srv, "/v1/intake/canonical/"+r3.ID+"/replay")
	if replayResp.StatusCode != 200 {
		t.Fatalf("replay: %d %s", replayResp.StatusCode, replayData)
	}
	var snap, reconstructed struct {
		Accommodations []string `json:"accommodations"`
		Consent        []string `json:"consent"`
		ContactMasked  bool     `json:"contactMasked"`
	}
	_ = json.Unmarshal(snapData, &snap)
	var replayEnvelope struct {
		Reconstructed json.RawMessage `json:"reconstructed"`
	}
	_ = json.Unmarshal(replayData, &replayEnvelope)
	_ = json.Unmarshal(replayEnvelope.Reconstructed, &reconstructed)
	if !equalStrings(snap.Accommodations, reconstructed.Accommodations) ||
		!equalStrings(snap.Consent, reconstructed.Consent) ||
		snap.ContactMasked != reconstructed.ContactMasked {
		t.Fatalf("replay mismatch:\nsnapshot=%s\nreplay=%s", snapData, replayData)
	}
}

func postReplay(t *testing.T, srv *httptest.Server, path string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post replay: %v", err)
	}
	data, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, data
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 12. Health check and attempts endpoint.
func TestHealthAndAttempts(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, data := get(t, srv, "/healthz")
	if resp.StatusCode != 200 || !bytes.Contains(data, []byte(`"ok"`)) {
		t.Fatalf("health: %d %s", resp.StatusCode, data)
	}
	postEvent(t, srv, map[string]any{
		"eventId": "EV-H", "channel": "WEB", "submissionId": "W-H",
		"applicant": map[string]any{"fullName": "Health User", "email": "h@example.com"},
	})
	resp, data = get(t, srv, "/v1/intake/events/EV-H")
	if resp.StatusCode != 200 {
		t.Fatalf("get event: %d %s", resp.StatusCode, data)
	}
}
