package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/accessible-intake-gateway/internal/contract"
	"github.com/accessible-intake-gateway/internal/intake"
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

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	c, err := contract.Parse([]byte(fixture))
	if err != nil {
		t.Fatalf("contract: %v", err)
	}
	st, err := store.Open("file:httpapi_test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	srv := httptest.NewServer(New(intake.New(c, st, nil)).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPEndToEnd(t *testing.T) {
	srv := newTestServer(t)

	body := `{"events":[
		{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER"],"canonicalKey":"APP-H"},
		{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"],"canonicalKey":"APP-H"},
		{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER"],"canonicalKey":"APP-H"},
		{"eventId":"EV-BAD","channel":"WEB","canonicalKey":"APP-H"}
	]}`
	resp, err := http.Post(srv.URL+"/v1/intake/events", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out submitResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Summary.Accepted != 2 || out.Summary.Duplicate != 1 || out.Summary.Rejected != 1 {
		t.Fatalf("unexpected batch summary: %+v", out.Summary)
	}

	// Fetch the canonical request.
	r2, err := http.Get(srv.URL + "/v1/requests/APP-H")
	if err != nil {
		t.Fatalf("get request: %v", err)
	}
	defer r2.Body.Close()
	var req model.CanonicalRequest
	if err := json.NewDecoder(r2.Body).Decode(&req); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if len(req.Chain) != 2 {
		t.Fatalf("expected chain of 2, got %d", len(req.Chain))
	}

	// Fetch the audit summary.
	r3, err := http.Get(srv.URL + "/v1/requests/APP-H/summary")
	if err != nil {
		t.Fatalf("get summary: %v", err)
	}
	defer r3.Body.Close()
	var sum model.AuditSummary
	if err := json.NewDecoder(r3.Body).Decode(&sum); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if sum.Digest == "" || sum.AcceptedCount != 2 {
		t.Fatalf("unexpected summary: digest=%q accepted=%d", sum.Digest, sum.AcceptedCount)
	}

	// Health endpoint.
	r4, err := http.Get(srv.URL + "/healthz")
	if err != nil || r4.StatusCode != http.StatusOK {
		t.Fatalf("health failed: %v status=%v", err, r4)
	}
	r4.Body.Close()
}
