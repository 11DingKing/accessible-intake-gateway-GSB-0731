package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	svc, _ := newTestService(t)
	srv := httptest.NewServer(NewHandler(svc, testRegistry(t)))
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url string, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, data
}

func get(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, data
}

type batchResponse struct {
	Results []RecordResult `json:"results"`
}

// TestEndToEndMixedBatch drives a batch mixing the fixture examples with
// out-of-order, duplicate, incomplete, unknown-code and revocation records,
// then replays the audit surface twice for byte-stability.
func TestEndToEndMixedBatch(t *testing.T) {
	srv := newTestServer(t)

	batch := `{"records":[
		{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","visitor":{"name":"Li Ming","phone":"+86 138-0000-0000"},"accommodations":["BRAILLE_MATERIAL"]},
		{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]},
		{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","applicant":{"name":"Li Ming","phone":"+8613800000000"},"consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]},
		{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]},
		{"eventId":"EV-B-001","channel":"HOTLINE","callRef":"C-99","callbackWindowMinutes":-10},
		{"eventId":"EV-B-002","channel":"PHYSICAL","deskReceiptNo":"D-99","accommodations":["HOLOGRAM"]},
		{"eventId":"EV-B-003","channel":"PIGEON","submissionId":"W-99"},
		{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","visitor":{"name":"Li Ming","phone":"+86 138-0000-0000"},"accommodations":["BRAILLE_MATERIAL"]},
		{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":60,"caller":{"name":"Li Ming","phone":"13800000000"},"consent":["CONTACT_CALLBACK"]},
		{"eventId":"EV-R-001","revokes":"EV-FUTURE","scopes":["CONTACT_CALLBACK"]},
		{"channel":"WEB","submissionId":"W-88"}
	]}`
	status, body := post(t, srv.URL+"/v1/intake/batches", batch)
	if status != http.StatusOK {
		t.Fatalf("batch status = %d, body = %s", status, body)
	}
	var br batchResponse
	if err := json.Unmarshal(body, &br); err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if len(br.Results) != 11 {
		t.Fatalf("expected 11 per-record results, got %d", len(br.Results))
	}

	wantStatus := []string{
		StatusAccepted, StatusAccepted, StatusAccepted, StatusAccepted, // merged trio + revocation
		StatusFailed, StatusFailed, StatusFailed, // negative window, unknown code, unknown channel
		StatusAccepted, // duplicate replay of EV-P-001 returns the original ACCEPTED result
		StatusConflict, // EV-H-001 with a changed payload
		StatusFailed,   // out-of-order revocation of EV-FUTURE
		StatusFailed,   // missing eventId
	}
	for i, want := range wantStatus {
		if br.Results[i].Status != want {
			t.Fatalf("record %d: want %s, got %+v", i, want, br.Results[i])
		}
	}
	// The three channels merged into one canonical request.
	merged := br.Results[0].RequestID
	if br.Results[1].RequestID != merged || br.Results[2].RequestID != merged {
		t.Fatalf("channels did not merge: %+v", br.Results[:3])
	}
	if br.Results[3].RequestID != merged || len(br.Results[3].AppliedScopes) != 1 {
		t.Fatalf("revocation result = %+v", br.Results[3])
	}
	if !br.Results[7].IdempotentReplay {
		t.Fatalf("record 7 must be flagged as replay: %+v", br.Results[7])
	}
	if br.Results[8].Errors[0].Code != CodePayloadConflict {
		t.Fatalf("record 8 = %+v", br.Results[8])
	}
	if !br.Results[9].Retryable || br.Results[9].Errors[0].Code != CodeRevokesUnknownEvent {
		t.Fatalf("record 9 = %+v", br.Results[9])
	}
	if br.Results[4].Errors[0].Code != CodeInvalidCallbackWindow ||
		br.Results[5].Errors[0].Code != CodeUnknownAccommodation ||
		br.Results[6].Errors[0].Code != CodeUnknownChannel ||
		br.Results[10].Errors[0].Code != CodeMissingEventID {
		t.Fatalf("per-record error codes wrong: %+v", br.Results[4:])
	}

	// Canonical projection: merged channels, revoked scope gone, contact
	// visible because CONTACT_CALLBACK is still effective.
	status, viewBody := get(t, srv.URL+"/v1/requests/"+merged)
	if status != http.StatusOK {
		t.Fatalf("get request status = %d", status)
	}
	var view RequestView
	if err := json.Unmarshal(viewBody, &view); err != nil {
		t.Fatalf("decode view: %v", err)
	}
	if view.EventCount != 4 || len(view.Channels) != 3 {
		t.Fatalf("view = %+v", view)
	}
	if contains(view.ConsentEffective, "CASE_SUMMARY_TRANSFER") ||
		!contains(view.ConsentEffective, "CONTACT_CALLBACK") ||
		!contains(view.ConsentEffective, "ACCOMMODATION_TRANSFER") {
		t.Fatalf("effective consent = %v", view.ConsentEffective)
	}
	if view.Contact == nil || view.Contact.Phone == "" {
		t.Fatalf("contact must be visible: %+v", view)
	}
	if view.Channels["HOTLINE"].CallbackWindowMinutes == nil || *view.Channels["HOTLINE"].CallbackWindowMinutes != 45 {
		t.Fatalf("hotline callback window = %+v", view.Channels["HOTLINE"])
	}

	// Audit summary, event chain and attempts must replay byte-identically.
	for _, path := range []string{"/audit", "/events", "/attempts"} {
		status, first := get(t, srv.URL+"/v1/requests/"+merged+path)
		if status != http.StatusOK {
			t.Fatalf("get %s status = %d", path, status)
		}
		_, second := get(t, srv.URL+"/v1/requests/"+merged+path)
		if !bytes.Equal(first, second) {
			t.Fatalf("%s not stably replayable", path)
		}
	}
	status, eventsBody := get(t, srv.URL+"/v1/requests/"+merged+"/events")
	var eventsPayload struct {
		Events []EventView `json:"events"`
	}
	if err := json.Unmarshal(eventsBody, &eventsPayload); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	if len(eventsPayload.Events) != 4 {
		t.Fatalf("chain length = %d", len(eventsPayload.Events))
	}
	for i := 1; i < len(eventsPayload.Events); i++ {
		if eventsPayload.Events[i].Seq <= eventsPayload.Events[i-1].Seq {
			t.Fatal("event chain order unstable")
		}
	}
	status, attemptsBody := get(t, srv.URL+"/v1/requests/"+merged+"/attempts")
	var attemptsPayload struct {
		Attempts []AttemptView `json:"attempts"`
	}
	if err := json.Unmarshal(attemptsBody, &attemptsPayload); err != nil {
		t.Fatalf("decode attempts: %v", err)
	}
	// accepted x4, replay x1 (EV-P-001), conflict x1 (EV-H-001 changed payload)
	if len(attemptsPayload.Attempts) != 6 {
		t.Fatalf("attempts = %+v", attemptsPayload.Attempts)
	}
	_ = status
}

// TestSingleEndpointStatuses pins the single-record endpoint's status codes.
func TestSingleEndpointStatuses(t *testing.T) {
	srv := newTestServer(t)

	status, body := post(t, srv.URL+"/v1/intake/events", physicalEvent)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d", status)
	}
	var created RecordResult
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode: %v", err)
	}

	status, body = post(t, srv.URL+"/v1/intake/events", physicalEvent)
	if status != http.StatusOK {
		t.Fatalf("replay status = %d", status)
	}
	var replay RecordResult
	if err := json.Unmarshal(body, &replay); err != nil || !replay.IdempotentReplay {
		t.Fatalf("replay = %s", body)
	}

	status, _ = post(t, srv.URL+"/v1/intake/events", `{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","accommodations":["TEXT_ONLY"]}`)
	if status != http.StatusConflict {
		t.Fatalf("conflict status = %d", status)
	}

	status, _ = post(t, srv.URL+"/v1/intake/events", `{not json`)
	if status != http.StatusBadRequest {
		t.Fatalf("bad json status = %d", status)
	}

	status, _ = post(t, srv.URL+"/v1/intake/events", `{"eventId":"EV-V-1","channel":"WEB","submissionId":"W-1","consent":["NOPE"]}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("validation status = %d", status)
	}

	status, _ = get(t, srv.URL+"/v1/requests/req_missing")
	if status != http.StatusNotFound {
		t.Fatalf("missing request status = %d", status)
	}

	status, body = get(t, srv.URL+"/healthz")
	if status != http.StatusOK || !bytes.Contains(body, []byte("intake.v1")) {
		t.Fatalf("healthz = %d %s", status, body)
	}
}
