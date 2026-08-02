package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// TestCallbackWindowKinds pins the deterministic classification: zero →
// IMMEDIATE, 1..1440 → SAME_DAY, >1440 → CROSS_DAY, negative → rejected.
func TestCallbackWindowKinds(t *testing.T) {
	cases := []struct {
		minutes int64
		kind    string
	}{
		{0, CallbackImmediate},
		{45, CallbackSameDay},
		{1440, CallbackSameDay},
		{1441, CallbackCrossDay},
		{2880, CallbackCrossDay},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d→%s", tc.minutes, tc.kind), func(t *testing.T) {
			svc, _ := newTestService(t)
			raw := fmt.Sprintf(`{"eventId":"EV-W-%d","channel":"HOTLINE","callRef":"C-%d","callbackWindowMinutes":%d}`, tc.minutes, tc.minutes, tc.minutes)
			res := mustAccepted(t, svc, raw)
			view, err := svc.GetRequest(context.Background(), res.RequestID)
			if err != nil || view == nil {
				t.Fatalf("get request: %v", err)
			}
			if view.CallbackWindowMinutes == nil || *view.CallbackWindowMinutes != tc.minutes {
				t.Fatalf("minutes = %v, want %d", view.CallbackWindowMinutes, tc.minutes)
			}
			if view.CallbackWindowKind != tc.kind {
				t.Fatalf("view kind = %q, want %q", view.CallbackWindowKind, tc.kind)
			}
			if got := view.Channels["HOTLINE"].CallbackWindowKind; got != tc.kind {
				t.Fatalf("link kind = %q, want %q", got, tc.kind)
			}
			audit, _ := svc.GetAudit(context.Background(), res.RequestID)
			if audit.CallbackWindowKind != tc.kind || audit.CallbackWindowMinutes == nil || *audit.CallbackWindowMinutes != tc.minutes {
				t.Fatalf("audit window = %+v", audit)
			}
		})
	}

	svc, _ := newTestService(t)
	res, status := mustProcess(t, svc, `{"eventId":"EV-NEG","channel":"HOTLINE","callRef":"C-1","callbackWindowMinutes":-1}`)
	if status != http.StatusUnprocessableEntity || res.Errors[0].Code != CodeInvalidCallbackWindow {
		t.Fatalf("negative window must stay rejected: %d %+v", status, res)
	}
}

// TestCallbackBeforeWebRegistration: the hotline callback event lands before
// the web record exists; the later web event merges onto the same chain via
// deterministic identity evidence (same phone, different name order).
func TestCallbackBeforeWebRegistration(t *testing.T) {
	svc, store := newTestService(t)
	hotline := mustAccepted(t, svc, `{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`)
	if hotline.Match == nil || hotline.Match.Decision != MatchNew {
		t.Fatalf("first event must start a new request: %+v", hotline.Match)
	}

	web := mustAccepted(t, svc, `{"eventId":"EV-W-009","channel":"WEB","submissionId":"W-9","applicant":{"name":"Ming Li","phone":"+8613800000000"},"consent":["CASE_SUMMARY_TRANSFER"]}`)
	if web.RequestID != hotline.RequestID {
		t.Fatalf("web event must merge into the hotline request: %s vs %s", web.RequestID, hotline.RequestID)
	}
	if web.Match == nil || web.Match.Decision != MatchMerged {
		t.Fatalf("decision = %+v", web.Match)
	}
	if web.Match.Score != scorePhoneMatch {
		t.Fatalf("score = %d, want %d (phone match only)", web.Match.Score, scorePhoneMatch)
	}
	if len(web.Match.Considered) != 1 || web.Match.Considered[0].Outcome != outcomeMerge {
		t.Fatalf("considered = %+v", web.Match.Considered)
	}
	assertOnlyHashEvidence(t, web.Match, "+8613800000000", "13800000000")

	var requestCount int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM canonical_requests`).Scan(&requestCount); err != nil {
		t.Fatal(err)
	}
	if requestCount != 1 {
		t.Fatalf("expected exactly one canonical request, got %d", requestCount)
	}
	events, _ := store.ListEvents(context.Background(), hotline.RequestID)
	if len(events) != 2 || events[0].EventID != "EV-H-001" || events[1].EventID != "EV-W-009" {
		t.Fatalf("chain = %+v", events)
	}
}

// TestPartialIdentityConflict: same name but a conflicting phone fragment —
// the event must NOT merge; it becomes a standalone request whose result
// carries the deterministic rejection rationale and hashed evidence.
func TestPartialIdentityConflict(t *testing.T) {
	svc, store := newTestService(t)
	base := mustAccepted(t, svc, `{"eventId":"EV-P-100","channel":"PHYSICAL","deskReceiptNo":"D-1","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`)

	conflicting := mustAccepted(t, svc, `{"eventId":"EV-H-100","channel":"HOTLINE","callRef":"C-100","callbackWindowMinutes":30,"caller":{"name":"Li Ming","phone":"+86 139 1111 1111"}}`)
	if conflicting.RequestID == base.RequestID {
		t.Fatal("conflicting strong identifier must not merge")
	}
	m := conflicting.Match
	if m == nil || m.Decision != MatchCandidate {
		t.Fatalf("decision = %+v", m)
	}
	if len(m.Considered) != 1 || m.Considered[0].RequestID != base.RequestID || m.Considered[0].Outcome != outcomeConflictRejected {
		t.Fatalf("considered = %+v", m.Considered)
	}
	c := m.Considered[0]
	if len(c.Matched) != 1 || c.Matched[0].Kind != identityName {
		t.Fatalf("matched = %+v", c.Matched)
	}
	if len(c.Conflicted) != 1 || c.Conflicted[0].Kind != identityPhone {
		t.Fatalf("conflicted = %+v", c.Conflicted)
	}
	if c.Conflicted[0].FragmentHash != fragmentHash(identityPhone, "8613911111111") {
		t.Fatalf("unexpected fragment hash: %+v", c.Conflicted[0])
	}
	if c.Score != scoreNameMatch+penaltyStrongConflict {
		t.Fatalf("score = %d", c.Score)
	}
	joined := strings.Join(m.Rationale, " | ")
	if !strings.Contains(joined, base.RequestID) || !strings.Contains(joined, "phone conflict") {
		t.Fatalf("rationale = %v", m.Rationale)
	}
	assertOnlyHashEvidence(t, m, "861380000000", "8613911111111", "+86 138 0000 0000", "+86 139 1111 1111")

	var requestCount int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM canonical_requests`).Scan(&requestCount); err != nil {
		t.Fatal(err)
	}
	if requestCount != 2 {
		t.Fatalf("conflicting identities must stay two requests, got %d", requestCount)
	}
}

// TestNameOnlyCandidate: a name-only overlap is recorded as a candidate with
// rationale but never merged.
func TestNameOnlyCandidate(t *testing.T) {
	svc, _ := newTestService(t)
	base := mustAccepted(t, svc, `{"eventId":"EV-P-200","channel":"PHYSICAL","deskReceiptNo":"D-2","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`)

	byEmail := mustAccepted(t, svc, `{"eventId":"EV-W-200","channel":"WEB","submissionId":"W-2","applicant":{"name":"li  MING","email":"li@example.org"}}`)
	if byEmail.RequestID == base.RequestID {
		t.Fatal("name-only overlap must not merge")
	}
	m := byEmail.Match
	if m == nil || m.Decision != MatchCandidate || len(m.Considered) != 1 || m.Considered[0].Outcome != outcomeNameOnly {
		t.Fatalf("match = %+v", m)
	}
	if m.Considered[0].Score != scoreNameMatch {
		t.Fatalf("score = %d", m.Considered[0].Score)
	}
}

// TestIDNumberConflictBlocksMerge: a matching phone cannot override a
// conflicting idNumber — the hard block wins.
func TestIDNumberConflictBlocksMerge(t *testing.T) {
	svc, _ := newTestService(t)
	base := mustAccepted(t, svc, `{"eventId":"EV-P-300","channel":"PHYSICAL","deskReceiptNo":"D-3","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000","idNumber":"ID-A"}}`)

	event := mustAccepted(t, svc, `{"eventId":"EV-W-300","channel":"WEB","submissionId":"W-3","applicant":{"name":"Li Ming","phone":"+8613800000000","idNumber":"ID-B"}}`)
	if event.RequestID == base.RequestID {
		t.Fatal("idNumber conflict must block the merge despite phone match")
	}
	m := event.Match
	if m == nil || m.Decision != MatchCandidate || len(m.Considered) != 1 || m.Considered[0].Outcome != outcomeConflictRejected {
		t.Fatalf("match = %+v", m)
	}
	want := scoreNameMatch + scorePhoneMatch + penaltyIDConflict
	if m.Considered[0].Score != want {
		t.Fatalf("score = %d, want %d", m.Considered[0].Score, want)
	}
}

// TestAccumulatedFragmentsEnableLaterMatch: identity fragments brought by a
// merged event are registered, so a later event can match on them.
func TestAccumulatedFragmentsEnableLaterMatch(t *testing.T) {
	svc, _ := newTestService(t)
	first := mustAccepted(t, svc, `{"eventId":"EV-A-1","channel":"PHYSICAL","deskReceiptNo":"D-4","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`)
	second := mustAccepted(t, svc, `{"eventId":"EV-A-2","channel":"WEB","submissionId":"W-4","applicant":{"name":"Ming Li","phone":"+8613800000000","email":"li@example.org"}}`)
	if second.Match == nil || second.Match.Decision != MatchMerged || second.RequestID != first.RequestID {
		t.Fatalf("second = %+v", second.Match)
	}
	// Matches only via the email fragment contributed by EV-A-2.
	third := mustAccepted(t, svc, `{"eventId":"EV-A-3","channel":"HOTLINE","callRef":"C-4","caller":{"name":"li ming","email":"LI@example.org"}}`)
	if third.RequestID != first.RequestID {
		t.Fatalf("third event must merge via accumulated email fragment: %s vs %s", third.RequestID, first.RequestID)
	}
	if third.Match == nil || third.Match.Decision != MatchMerged {
		t.Fatalf("third match = %+v", third.Match)
	}
	foundEmail := false
	for _, ev := range third.Match.Considered[0].Matched {
		if ev.Kind == identityEmail {
			foundEmail = true
		}
	}
	if !foundEmail {
		t.Fatalf("expected email evidence: %+v", third.Match.Considered[0].Matched)
	}
}

// TestConcurrentFuzzyClaim: two channels concurrently claiming the same
// person with differently-ordered names form exactly one chain.
func TestConcurrentFuzzyClaim(t *testing.T) {
	svc, store := newTestService(t)
	raws := []string{
		`{"eventId":"EV-CC-1","channel":"PHYSICAL","deskReceiptNo":"D-1","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`,
		`{"eventId":"EV-CC-2","channel":"WEB","submissionId":"W-1","applicant":{"name":"Ming Li","phone":"+8613800000000"}}`,
		`{"eventId":"EV-CC-3","channel":"HOTLINE","callRef":"C-1","callbackWindowMinutes":15,"caller":{"name":"LI MING","phone":"+86-138-0000-0000"}}`,
		`{"eventId":"EV-CC-4","channel":"WEB","submissionId":"W-2","applicant":{"name":"ming li","phone":"138 0000 0000"}}`,
	}
	// note: EV-CC-4 phone "138 0000 0000" normalizes without country code and
	// must not merge — it exercises a concurrent CANDIDATE, not a chain fork.
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

	merged := map[string]int{}
	for _, r := range results {
		merged[r.RequestID]++
	}
	if len(merged) != 2 {
		t.Fatalf("expected one merged chain plus one standalone candidate, got %+v", merged)
	}
	var mainID string
	for id, n := range merged {
		if n == 3 {
			mainID = id
		}
	}
	if mainID == "" {
		t.Fatalf("no 3-event merged chain: %+v", merged)
	}
	events, _ := store.ListEvents(context.Background(), mainID)
	if len(events) != 3 {
		t.Fatalf("merged chain length = %d", len(events))
	}
	// Timing-independent guarantee: EV-CC-4 shares no strong identifier, so
	// it never merges — whether it saw the accumulated "ming li" name
	// fragment (CANDIDATE) or not (NEW) depends on transaction interleaving.
	if results[3].Match == nil ||
		(results[3].Match.Decision != MatchCandidate && results[3].Match.Decision != MatchNew) {
		t.Fatalf("EV-CC-4 must stay standalone (CANDIDATE or NEW): %+v", results[3].Match)
	}
	if results[3].RequestID == mainID {
		t.Fatal("EV-CC-4 must not join the merged chain")
	}
}

// TestFuzzyRetryNoSecondChain: adapter failure recovery on a fuzzy-matching
// event still converges to one chain.
func TestFuzzyRetryNoSecondChain(t *testing.T) {
	svc, store := newTestService(t)
	var mu sync.Mutex
	failedOnce := false
	svc.SetTransientHook(func(eventID string, raw []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if eventID == "EV-T-H" && !failedOnce {
			failedOnce = true
			return errors.New("hotline adapter timeout")
		}
		return nil
	})

	hotlineRaw := `{"eventId":"EV-T-H","channel":"HOTLINE","callRef":"C-5","callbackWindowMinutes":20,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`
	if _, status := mustProcess(t, svc, hotlineRaw); status != http.StatusServiceUnavailable {
		t.Fatalf("first attempt status = %d", status)
	}
	web := mustAccepted(t, svc, `{"eventId":"EV-T-W","channel":"WEB","submissionId":"W-5","applicant":{"name":"Ming Li","phone":"+8613800000000"}}`)
	retry := mustAccepted(t, svc, hotlineRaw)
	if retry.RequestID != web.RequestID || retry.Match == nil || retry.Match.Decision != MatchMerged {
		t.Fatalf("retry must merge into the web request: %+v", retry)
	}

	var requestCount int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM canonical_requests`).Scan(&requestCount); err != nil {
		t.Fatal(err)
	}
	if requestCount != 1 {
		t.Fatalf("failure recovery forked chains: %d requests", requestCount)
	}
	attempts, _ := store.ListAttemptsForRequest(context.Background(), web.RequestID)
	if len(attempts) != 3 || attempts[0].Outcome != AttemptTransientFailure {
		t.Fatalf("attempts = %+v", attempts)
	}
}

// TestOutOfOrderReplayAfterMerge: replays arriving after a fuzzy merge
// return stored results and never fold a second time.
func TestOutOfOrderReplayAfterMerge(t *testing.T) {
	svc, store := newTestService(t)
	webRaw := `{"eventId":"EV-O-W","channel":"WEB","submissionId":"W-6","applicant":{"name":"Ming Li","phone":"+8613800000000"}}`
	hotlineRaw := `{"eventId":"EV-O-H","channel":"HOTLINE","callRef":"C-6","callbackWindowMinutes":0,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`
	web := mustAccepted(t, svc, webRaw)
	hotline := mustAccepted(t, svc, hotlineRaw)
	if hotline.RequestID != web.RequestID || hotline.Match.Decision != MatchMerged {
		t.Fatalf("setup merge failed: %+v %+v", web.Match, hotline.Match)
	}

	for i, raw := range []string{hotlineRaw, webRaw} {
		replay, status := mustProcess(t, svc, raw)
		if status != http.StatusOK || !replay.IdempotentReplay {
			t.Fatalf("replay %d: %d %+v", i, status, replay)
		}
		replay.IdempotentReplay = false
		original := web
		if i == 0 {
			original = hotline
		}
		wantJSON, _ := json.Marshal(original)
		gotJSON, _ := json.Marshal(replay)
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("replay %d diverged:\nwant %s\ngot  %s", i, wantJSON, gotJSON)
		}
	}
	events, _ := store.ListEvents(context.Background(), web.RequestID)
	if len(events) != 2 {
		t.Fatalf("replays must not append events: %d", len(events))
	}
}

// TestCallbackDuplicateAndConflict: duplicate key replays; same key with a
// different callback window conflicts explicitly.
func TestCallbackDuplicateAndConflict(t *testing.T) {
	svc, _ := newTestService(t)
	raw := `{"eventId":"EV-D-1","channel":"HOTLINE","callRef":"C-7","callbackWindowMinutes":0,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`
	first := mustAccepted(t, svc, raw)
	replay, status := mustProcess(t, svc, raw)
	if status != http.StatusOK || !replay.IdempotentReplay || replay.RequestID != first.RequestID {
		t.Fatalf("duplicate callback key must replay: %d %+v", status, replay)
	}
	conflict, status := mustProcess(t, svc, `{"eventId":"EV-D-1","channel":"HOTLINE","callRef":"C-7","callbackWindowMinutes":30,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`)
	if status != http.StatusConflict || conflict.Errors[0].Code != CodePayloadConflict {
		t.Fatalf("changed window on same key must conflict: %d %+v", status, conflict)
	}
}

// TestContactNotExposedBeforeConsent: before CONTACT_CALLBACK is granted the
// projection hides raw contact details, and match evidence never carries raw
// contact values — even for the merging event that brought them.
func TestContactNotExposedBeforeConsent(t *testing.T) {
	svc, _ := newTestService(t)
	first := mustAccepted(t, svc, `{"eventId":"EV-N-1","channel":"PHYSICAL","deskReceiptNo":"D-8","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`)
	view, _ := svc.GetRequest(context.Background(), first.RequestID)
	if view.Contact != nil {
		t.Fatalf("contact exposed before consent: %+v", view.Contact)
	}

	second := mustAccepted(t, svc, `{"eventId":"EV-N-2","channel":"HOTLINE","callRef":"C-8","callbackWindowMinutes":10,"caller":{"name":"Ming Li","phone":"+8613800000000"},"consent":["CONTACT_CALLBACK"]}`)
	if second.Match == nil || second.Match.Decision != MatchMerged {
		t.Fatalf("second = %+v", second.Match)
	}
	assertOnlyHashEvidence(t, second.Match, "861380000000", "+8613800000000", "+86 138 0000 0000")

	view, _ = svc.GetRequest(context.Background(), first.RequestID)
	if view.Contact == nil || view.Contact.Phone == "" {
		t.Fatalf("contact must appear once CONTACT_CALLBACK is effective: %+v", view)
	}
}

// TestMatchEvidenceDeterministic: the same arrival sequence produces the
// same match decisions, scores, evidence and rationale (modulo generated
// request IDs) in independent databases.
func TestMatchEvidenceDeterministic(t *testing.T) {
	script := func(t *testing.T) []RecordResult {
		svc, _ := newTestService(t)
		r1 := mustAccepted(t, svc, `{"eventId":"EV-S-1","channel":"PHYSICAL","deskReceiptNo":"D-9","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"}}`)
		r2 := mustAccepted(t, svc, `{"eventId":"EV-S-2","channel":"HOTLINE","callRef":"C-9","callbackWindowMinutes":5,"caller":{"name":"Li Ming","phone":"+86 139 9999 9999"}}`)
		r3 := mustAccepted(t, svc, `{"eventId":"EV-S-3","channel":"WEB","submissionId":"W-9","applicant":{"name":"Ming Li","phone":"+8613800000000"}}`)
		return []RecordResult{r1, r2, r3}
	}
	runA := script(t)
	runB := script(t)
	normalize := func(results []RecordResult) string {
		aliases := map[string]string{}
		next := 0
		var b strings.Builder
		for _, r := range results {
			data, _ := json.Marshal(r.Match)
			s := string(data)
			for _, id := range []string{r.RequestID} {
				if _, ok := aliases[id]; !ok {
					next++
					aliases[id] = fmt.Sprintf("REQ_%d", next)
				}
			}
			for id, alias := range aliases {
				s = strings.ReplaceAll(s, id, alias)
			}
			b.WriteString(s)
			b.WriteString("\n")
		}
		return b.String()
	}
	if normalize(runA) != normalize(runB) {
		t.Fatalf("match evidence not deterministic:\nA: %s\nB: %s", normalize(runA), normalize(runB))
	}
	if runA[1].Match.Decision != MatchCandidate || runA[2].Match.Decision != MatchMerged || runA[0].Match.Decision != MatchNew {
		t.Fatalf("decisions = %s %s %s", runA[0].Match.Decision, runA[1].Match.Decision, runA[2].Match.Decision)
	}
}

// TestMigrationBackfillsIdentities: a database created at schema v1 gains
// identity fragments for its existing requests when v2 migrates, so fuzzy
// matching works against pre-v2 records.
func TestMigrationBackfillsIdentities(t *testing.T) {
	store := openMemoryStore(t)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	)`); err != nil {
		t.Fatal(err)
	}
	if err := migrations[0].apply(ctx, store.db); err != nil {
		t.Fatalf("apply v1: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	stateJSON := string(mustMarshal(newRequestState("intake.v1")))
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO canonical_requests (request_id, person_key, person_json, state_json, event_count, created_at, updated_at) VALUES (?,?,?,?,0,?,?)`,
		"req_legacy", "person:legacy", `{"name":"Li Ming","phone":"+86 138 0000 0000"}`, stateJSON, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate to v2: %v", err)
	}
	var fragCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM request_identities WHERE request_id = 'req_legacy'`).Scan(&fragCount); err != nil {
		t.Fatalf("count fragments: %v", err)
	}
	if fragCount != 2 { // name + phone
		t.Fatalf("backfilled fragments = %d", fragCount)
	}

	svc := NewService(store, testRegistry(t))
	merged := mustAccepted(t, svc, `{"eventId":"EV-L-1","channel":"HOTLINE","callRef":"C-10","caller":{"name":"Ming Li","phone":"+8613800000000"}}`)
	if merged.RequestID != "req_legacy" || merged.Match == nil || merged.Match.Decision != MatchMerged {
		t.Fatalf("fuzzy match against pre-v2 request failed: %+v", merged)
	}
}

// assertOnlyHashEvidence fails when any raw identity fragment leaks into the
// serialized match evidence.
func assertOnlyHashEvidence(t *testing.T, m *MatchEvidence, rawValues ...string) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal match: %v", err)
	}
	for _, raw := range rawValues {
		if strings.Contains(string(data), raw) {
			t.Fatalf("match evidence leaks raw value %q: %s", raw, data)
		}
	}
}
