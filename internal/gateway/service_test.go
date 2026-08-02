package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

const (
	physicalEvent = `{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","accommodations":["BRAILLE_MATERIAL"]}`
	hotlineEvent  = `{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"]}`
	webEvent      = `{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]}`
)

func mustProcess(t *testing.T, svc *Service, raw string) (RecordResult, int) {
	t.Helper()
	res, status := svc.ProcessEnvelope(context.Background(), []byte(raw))
	return res, status
}

func mustAccepted(t *testing.T, svc *Service, raw string) RecordResult {
	t.Helper()
	res, status := mustProcess(t, svc, raw)
	if status != http.StatusCreated || res.Status != StatusAccepted {
		t.Fatalf("expected ACCEPTED/201, got %d %+v", status, res)
	}
	return res
}

// TestFixtureExamplesAccepted posts the three canonical fixture envelopes.
func TestFixtureExamplesAccepted(t *testing.T) {
	svc, _ := newTestService(t)
	for _, raw := range []string{physicalEvent, hotlineEvent, webEvent} {
		res := mustAccepted(t, svc, raw)
		if res.RequestID == "" || res.Seq == 0 {
			t.Fatalf("result missing request linkage: %+v", res)
		}
	}
	// Without person identity the events must not be merged into one request.
	r1 := mustAccepted(t, svc, `{"eventId":"EV-P-002","channel":"PHYSICAL","deskReceiptNo":"D-89"}`)
	r2 := mustAccepted(t, svc, `{"eventId":"EV-W-002","channel":"WEB","submissionId":"W-72"}`)
	if r1.RequestID == r2.RequestID {
		t.Fatal("personless events must anchor standalone requests")
	}
}

// TestValidationErrorTaxonomy covers the fixture's invalid cases plus the
// missing-field and unknown-code classes, asserting per-record error codes.
func TestValidationErrorTaxonomy(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"negative callbackWindowMinutes", `{"eventId":"E1","channel":"HOTLINE","callRef":"C-1","callbackWindowMinutes":-5}`, []string{CodeInvalidCallbackWindow}},
		{"fractional callbackWindowMinutes", `{"eventId":"E2","channel":"HOTLINE","callRef":"C-1","callbackWindowMinutes":45.5}`, []string{CodeInvalidCallbackWindow}},
		{"unknown accommodation code", `{"eventId":"E3","channel":"PHYSICAL","deskReceiptNo":"D-1","accommodations":["HOLOGRAM"]}`, []string{CodeUnknownAccommodation}},
		{"unknown consent scope", `{"eventId":"E4","channel":"WEB","submissionId":"W-1","consent":["FULL_LOCATION"]}`, []string{CodeUnknownConsentScope}},
		{"unknown channel", `{"eventId":"E5","channel":"PIGEON","submissionId":"W-1"}`, []string{CodeUnknownChannel}},
		{"missing eventId", `{"channel":"WEB","submissionId":"W-1"}`, []string{CodeMissingEventID}},
		{"missing channel", `{"eventId":"E7","submissionId":"W-1"}`, []string{CodeMissingChannel}},
		{"missing source correlation", `{"eventId":"E8","channel":"PHYSICAL"}`, []string{CodeMissingCorrelationID}},
		{"callback window on wrong channel", `{"eventId":"E9","channel":"PHYSICAL","deskReceiptNo":"D-1","callbackWindowMinutes":10}`, []string{CodeInvalidFieldForChannel}},
		{"person without contact", `{"eventId":"E10","channel":"WEB","submissionId":"W-1","applicant":{"name":"Li Ming"}}`, []string{CodeInvalidPerson}},
		{"person without name", `{"eventId":"E11","channel":"WEB","submissionId":"W-1","applicant":{"phone":"13800000000"}}`, []string{CodeInvalidPerson}},
		{"revocation missing revokes", `{"eventId":"E12","eventType":"REVOCATION","scopes":["CONTACT_CALLBACK"]}`, []string{CodeMissingRevokes}},
		{"revocation missing scopes", `{"eventId":"E13","revokes":"EV-W-001"}`, []string{CodeMissingScopes}},
		{"revocation unknown scope", `{"eventId":"E14","revokes":"EV-W-001","scopes":["DNA_SAMPLE"]}`, []string{CodeUnknownConsentScope}},
		{"multiple errors collected", `{"eventId":"E15","channel":"HOTLINE","callRef":"C-1","callbackWindowMinutes":-1,"accommodations":["HOLOGRAM"],"consent":["BAD"]}`, []string{CodeInvalidCallbackWindow, CodeUnknownAccommodation, CodeUnknownConsentScope}},
		{"intake carrying revokes", `{"eventId":"E16","eventType":"INTAKE","channel":"WEB","submissionId":"W-1","revokes":"X"}`, []string{CodeInvalidEnvelope}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := newTestService(t)
			res, status := mustProcess(t, svc, tc.raw)
			if status != http.StatusUnprocessableEntity || res.Status != StatusFailed {
				t.Fatalf("expected FAILED/422, got %d %+v", status, res)
			}
			if res.Retryable {
				t.Fatalf("validation failures must not be retryable: %+v", res)
			}
			got := map[string]bool{}
			for _, e := range res.Errors {
				got[e.Code] = true
			}
			for _, code := range tc.want {
				if !got[code] {
					t.Fatalf("expected error %s in %+v", code, res.Errors)
				}
			}
		})
	}
}

// TestSameKeySamePayloadReplaysOriginal: byte-identical and key-reordered
// retries return the original stored result and add no new event.
func TestSameKeySamePayloadReplaysOriginal(t *testing.T) {
	svc, store := newTestService(t)
	first := mustAccepted(t, svc, hotlineEvent)

	reordered := `{"consent":["CONTACT_CALLBACK"],"callbackWindowMinutes":45,"callRef":"C-19","channel":"HOTLINE","eventId":"EV-H-001"}`
	replay, status := mustProcess(t, svc, reordered)
	if status != http.StatusOK {
		t.Fatalf("replay status = %d", status)
	}
	if !replay.IdempotentReplay {
		t.Fatalf("expected idempotentReplay flag: %+v", replay)
	}
	replay.IdempotentReplay = false // flag is replay-only; the rest must equal the original
	if fmt.Sprintf("%+v", replay) != fmt.Sprintf("%+v", first) {
		t.Fatalf("replay must return the original result:\nfirst:  %+v\nreplay: %+v", first, replay)
	}

	events, err := store.ListEvents(context.Background(), first.RequestID)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected exactly 1 event, got %d (%v)", len(events), err)
	}
	attempts, err := store.ListAttemptsForRequest(context.Background(), first.RequestID)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d (%v)", len(attempts), err)
	}
	if attempts[0].Outcome != AttemptAccepted || attempts[1].Outcome != AttemptReplayed {
		t.Fatalf("attempt outcomes = %s, %s", attempts[0].Outcome, attempts[1].Outcome)
	}
}

// TestSameKeyDifferentPayloadConflicts: the conflict is explicit, persists
// nothing but the attempt, and leaves the original chain untouched.
func TestSameKeyDifferentPayloadConflicts(t *testing.T) {
	svc, store := newTestService(t)
	first := mustAccepted(t, svc, webEvent)

	conflict, status := mustProcess(t, svc, `{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","consent":["CONTACT_CALLBACK"]}`)
	if status != http.StatusConflict || conflict.Status != StatusConflict {
		t.Fatalf("expected CONFLICT/409, got %d %+v", status, conflict)
	}
	if conflict.Errors[0].Code != CodePayloadConflict {
		t.Fatalf("expected PAYLOAD_CONFLICT, got %+v", conflict.Errors)
	}
	events, _ := store.ListEvents(context.Background(), first.RequestID)
	if len(events) != 1 {
		t.Fatalf("conflict must not append events, got %d", len(events))
	}
	attempts, _ := store.ListAttemptsForRequest(context.Background(), first.RequestID)
	if len(attempts) != 2 || attempts[1].Outcome != AttemptConflict {
		t.Fatalf("attempts = %+v", attempts)
	}
}

// TestFailedRecordDoesNotBlockRetry: a validation failure never "uses up"
// the event key; the corrected payload is processed on its own merits.
func TestFailedRecordDoesNotBlockRetry(t *testing.T) {
	svc, _ := newTestService(t)
	bad, status := mustProcess(t, svc, `{"eventId":"EV-F-1","channel":"HOTLINE","callRef":"C-77","callbackWindowMinutes":-3}`)
	if status != http.StatusUnprocessableEntity || bad.Status != StatusFailed {
		t.Fatalf("expected FAILED/422, got %d %+v", status, bad)
	}
	good := mustAccepted(t, svc, `{"eventId":"EV-F-1","channel":"HOTLINE","callRef":"C-77","callbackWindowMinutes":30}`)
	view, err := svc.GetRequest(context.Background(), good.RequestID)
	if err != nil || view == nil {
		t.Fatalf("get request: %v", err)
	}
	if view.CallbackWindowMinutes == nil || *view.CallbackWindowMinutes != 30 {
		t.Fatalf("callbackWindowMinutes = %v", view.CallbackWindowMinutes)
	}
}

// TestRevocationFlow: revocation removes exactly the targeted grant; replay
// of the original grant cannot resurrect the revoked scope.
func TestRevocationFlow(t *testing.T) {
	svc, store := newTestService(t)
	grant := mustAccepted(t, svc, webEvent)

	rev := mustAccepted(t, svc, `{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}`)
	if rev.RequestID != grant.RequestID {
		t.Fatal("revocation must land on the grant's request")
	}
	if len(rev.AppliedScopes) != 1 || rev.AppliedScopes[0] != "CASE_SUMMARY_TRANSFER" {
		t.Fatalf("appliedScopes = %v", rev.AppliedScopes)
	}
	if len(rev.EffectiveConsent) != 1 || rev.EffectiveConsent[0] != "ACCOMMODATION_TRANSFER" {
		t.Fatalf("effectiveConsent = %v", rev.EffectiveConsent)
	}

	// Retry of the original grant replays without resurrecting the scope.
	replay, status := mustProcess(t, svc, webEvent)
	if status != http.StatusOK || !replay.IdempotentReplay {
		t.Fatalf("expected replay, got %d %+v", status, replay)
	}
	view, _ := svc.GetRequest(context.Background(), grant.RequestID)
	if contains(view.ConsentEffective, "CASE_SUMMARY_TRANSFER") {
		t.Fatalf("revoked scope resurrected: %v", view.ConsentEffective)
	}
	if !contains(view.ConsentRevoked, "CASE_SUMMARY_TRANSFER") {
		t.Fatalf("revoked scope not reported: %v", view.ConsentRevoked)
	}

	// Retry of the revocation itself replays its original result.
	revReplay, status := mustProcess(t, svc, `{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}`)
	if status != http.StatusOK || !revReplay.IdempotentReplay {
		t.Fatalf("expected revocation replay, got %d %+v", status, revReplay)
	}
	events, _ := store.ListEvents(context.Background(), grant.RequestID)
	if len(events) != 2 || events[0].EventID != "EV-W-001" || events[1].EventID != "EV-W-002" {
		t.Fatalf("chain = %+v", events)
	}
}

// TestRevokedContactStaysHidden: once CONTACT_CALLBACK is revoked the
// contact projection disappears and no retry can expose it again.
func TestRevokedContactStaysHidden(t *testing.T) {
	svc, _ := newTestService(t)
	intake := `{"eventId":"EV-C-1","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138-0000-0000"},"consent":["CONTACT_CALLBACK"]}`
	grant := mustAccepted(t, svc, intake)

	view, _ := svc.GetRequest(context.Background(), grant.RequestID)
	if view.Contact == nil || view.Contact.Phone == "" {
		t.Fatalf("contact should be visible under CONTACT_CALLBACK: %+v", view)
	}

	mustAccepted(t, svc, `{"eventId":"EV-C-2","revokes":"EV-C-1","scopes":["CONTACT_CALLBACK"]}`)
	view, _ = svc.GetRequest(context.Background(), grant.RequestID)
	if view.Contact != nil {
		t.Fatalf("contact must be hidden after revocation: %+v", view.Contact)
	}
	if view.Person == nil || view.Person.Name != "Li Ming" {
		t.Fatalf("non-contact identity must remain: %+v", view.Person)
	}

	// Failed-retry of the original grant must not re-expose the phone.
	if _, status := mustProcess(t, svc, intake); status != http.StatusOK {
		t.Fatalf("expected replay, got %d", status)
	}
	view, _ = svc.GetRequest(context.Background(), grant.RequestID)
	if view.Contact != nil {
		t.Fatalf("retry resurrected revoked contact: %+v", view.Contact)
	}
}

// TestOutOfOrderRevocation: a revocation landing before its grant fails as
// retryable; after the grant arrives, the same revocation succeeds.
func TestOutOfOrderRevocation(t *testing.T) {
	svc, store := newTestService(t)
	revocation := `{"eventId":"EV-R-1","revokes":"EV-G-1","scopes":["CASE_SUMMARY_TRANSFER"]}`

	early, status := mustProcess(t, svc, revocation)
	if status != http.StatusUnprocessableEntity || early.Status != StatusFailed {
		t.Fatalf("expected FAILED/422, got %d %+v", status, early)
	}
	if !early.Retryable || early.Errors[0].Code != CodeRevokesUnknownEvent {
		t.Fatalf("expected retryable REVOKES_UNKNOWN_EVENT, got %+v", early)
	}

	grant := mustAccepted(t, svc, `{"eventId":"EV-G-1","channel":"WEB","submissionId":"W-9","consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]}`)
	late := mustAccepted(t, svc, revocation)
	if late.RequestID != grant.RequestID || len(late.AppliedScopes) != 1 {
		t.Fatalf("late revocation = %+v", late)
	}
	view, _ := svc.GetRequest(context.Background(), grant.RequestID)
	if contains(view.ConsentEffective, "CASE_SUMMARY_TRANSFER") {
		t.Fatalf("scope not revoked: %v", view.ConsentEffective)
	}
	attempts, _ := store.ListAttemptsForRequest(context.Background(), grant.RequestID)
	if len(attempts) != 3 {
		t.Fatalf("expected 3 attempts (failed revocation, grant, revocation), got %d", len(attempts))
	}
}

// TestTransientFailureThenRetry: a transient failure rolls the record back
// completely; the retry succeeds and the chain holds exactly one event.
func TestTransientFailureThenRetry(t *testing.T) {
	svc, store := newTestService(t)
	var mu sync.Mutex
	failedOnce := false
	svc.SetTransientHook(func(eventID string, raw []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if eventID == "EV-T-1" && !failedOnce {
			failedOnce = true
			return errors.New("adapter timeout")
		}
		return nil
	})

	raw := `{"eventId":"EV-T-1","channel":"WEB","submissionId":"W-55","consent":["ACCOMMODATION_TRANSFER"]}`
	fail, status := mustProcess(t, svc, raw)
	if status != http.StatusServiceUnavailable || fail.Status != StatusFailed || !fail.Retryable {
		t.Fatalf("expected retryable FAILED/503, got %d %+v", status, fail)
	}
	if fail.Errors[0].Code != CodeTransientFailure {
		t.Fatalf("expected TRANSIENT_FAILURE, got %+v", fail.Errors)
	}

	ok := mustAccepted(t, svc, raw)
	events, _ := store.ListEvents(context.Background(), ok.RequestID)
	if len(events) != 1 {
		t.Fatalf("retry must produce exactly one event, got %d", len(events))
	}
	attempts, _ := store.ListAttemptsForRequest(context.Background(), ok.RequestID)
	if len(attempts) != 2 || attempts[0].Outcome != AttemptTransientFailure || attempts[1].Outcome != AttemptAccepted {
		t.Fatalf("attempts = %+v", attempts)
	}
}

// TestCrossChannelPersonMerge: the applicant who visits the desk, calls the
// hotline and submits on the web lands on one canonical request.
func TestCrossChannelPersonMerge(t *testing.T) {
	svc, _ := newTestService(t)
	p1 := mustAccepted(t, svc, `{"eventId":"EV-M-1","channel":"PHYSICAL","deskReceiptNo":"D-88","visitor":{"name":"Li  Ming","phone":"+86 138-0000-0000"},"accommodations":["BRAILLE_MATERIAL"]}`)
	p2 := mustAccepted(t, svc, `{"eventId":"EV-M-2","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"li ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`)
	p3 := mustAccepted(t, svc, `{"eventId":"EV-M-3","channel":"WEB","submissionId":"W-71","applicant":{"name":"Li Ming","phone":"+8613800000000"},"consent":["CASE_SUMMARY_TRANSFER"]}`)

	if p1.RequestID != p2.RequestID || p2.RequestID != p3.RequestID {
		t.Fatalf("person events must merge into one request: %s %s %s", p1.RequestID, p2.RequestID, p3.RequestID)
	}
	view, err := svc.GetRequest(context.Background(), p1.RequestID)
	if err != nil || view == nil {
		t.Fatalf("get request: %v", err)
	}
	if view.EventCount != 3 {
		t.Fatalf("eventCount = %d", view.EventCount)
	}
	if len(view.Channels) != 3 {
		t.Fatalf("channels = %+v", view.Channels)
	}
	if view.Channels["PHYSICAL"].CorrelationID != "D-88" ||
		view.Channels["HOTLINE"].CorrelationID != "C-19" ||
		view.Channels["WEB"].CorrelationID != "W-71" {
		t.Fatalf("source correlations not preserved: %+v", view.Channels)
	}
	if view.CallbackWindowMinutes == nil || *view.CallbackWindowMinutes != 45 {
		t.Fatalf("callbackWindowMinutes = %v", view.CallbackWindowMinutes)
	}
	if view.Contact == nil || view.Contact.Phone == "" {
		t.Fatalf("contact must be visible under CONTACT_CALLBACK: %+v", view)
	}
}

// TestConcurrentChannelMergeRace: two channels reporting the same canonical
// request at the same time form exactly one event chain.
func TestConcurrentChannelMergeRace(t *testing.T) {
	svc, store := newTestService(t)
	raws := []string{
		`{"eventId":"EV-X-1","channel":"PHYSICAL","deskReceiptNo":"D-1","visitor":{"name":"Wang Fang","idNumber":"ID-1"}}`,
		`{"eventId":"EV-X-2","channel":"WEB","submissionId":"W-1","applicant":{"name":"Wang Fang","idNumber":"ID-1"}}`,
		`{"eventId":"EV-X-3","channel":"HOTLINE","callRef":"C-1","caller":{"name":"Wang Fang","idNumber":"ID-1"}}`,
		`{"eventId":"EV-X-4","channel":"PHYSICAL","deskReceiptNo":"D-2","visitor":{"name":"Wang Fang","idNumber":"ID-1"}}`,
	}
	results := make([]RecordResult, len(raws))
	var wg sync.WaitGroup
	for i, raw := range raws {
		wg.Add(1)
		go func(i int, raw string) {
			defer wg.Done()
			res, status := svc.ProcessEnvelope(context.Background(), []byte(raw))
			if status != http.StatusCreated {
				t.Errorf("status = %d for %s", status, raw)
			}
			results[i] = res
		}(i, raw)
	}
	wg.Wait()

	requestID := results[0].RequestID
	seqs := map[int64]bool{}
	for _, r := range results {
		if r.RequestID != requestID {
			t.Fatalf("concurrent reports split into multiple requests: %+v", results)
		}
		seqs[r.Seq] = true
	}
	if len(seqs) != len(raws) {
		t.Fatalf("duplicate sequence numbers on one chain: %+v", results)
	}
	events, err := store.ListEvents(context.Background(), requestID)
	if err != nil || len(events) != len(raws) {
		t.Fatalf("expected %d events on one chain, got %d (%v)", len(raws), len(events), err)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatal("event chain sequence not monotonic")
		}
	}
}

// TestConcurrentSameEvent: concurrent identical submissions are accepted
// exactly once; every other attempt replays the original result.
func TestConcurrentSameEvent(t *testing.T) {
	svc, store := newTestService(t)
	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	statuses := map[int]int{}
	var requestIDs = map[string]bool{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, status := svc.ProcessEnvelope(context.Background(), []byte(webEvent))
			mu.Lock()
			statuses[status]++
			requestIDs[res.RequestID] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if statuses[http.StatusCreated] != 1 || statuses[http.StatusOK] != n-1 {
		t.Fatalf("statuses = %+v", statuses)
	}
	if len(requestIDs) != 1 {
		t.Fatalf("requests = %+v", requestIDs)
	}
	var only string
	for id := range requestIDs {
		only = id
	}
	events, _ := store.ListEvents(context.Background(), only)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

// TestAuditSummaryStableAndComplete pins the minimal audit summary fields.
func TestAuditSummaryStableAndComplete(t *testing.T) {
	svc, _ := newTestService(t)
	grant := mustAccepted(t, svc, webEvent)
	mustAccepted(t, svc, `{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}`)

	audit, err := svc.GetAudit(context.Background(), grant.RequestID)
	if err != nil || audit == nil {
		t.Fatalf("get audit: %v", err)
	}
	if audit.CanonicalVersion != "intake.v1" || audit.EventCount != 2 {
		t.Fatalf("audit = %+v", audit)
	}
	if audit.Correlations["WEB"] != "W-71" {
		t.Fatalf("correlations = %+v", audit.Correlations)
	}
	if contains(audit.ConsentEffective, "CASE_SUMMARY_TRANSFER") || !contains(audit.ConsentRevoked, "CASE_SUMMARY_TRANSFER") {
		t.Fatalf("consent summary wrong: %+v", audit)
	}
	if audit.StateHash == "" || audit.FirstSeq == 0 || audit.LastSeq <= audit.FirstSeq {
		t.Fatalf("audit replay anchors missing: %+v", audit)
	}

	again, _ := svc.GetAudit(context.Background(), grant.RequestID)
	if fmt.Sprintf("%+v", audit) != fmt.Sprintf("%+v", again) {
		t.Fatal("audit summary must be stable across reads")
	}
}
