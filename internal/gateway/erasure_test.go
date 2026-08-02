package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
)

const revokedPhoneDigits = "8613800000000" // digitsOnly("+86 138 0000 0000")

// erasureFixture builds the merged chain used by most erasure tests:
// EV-W-001 (web, 2 scopes) + EV-H-001 (hotline, CONTACT_CALLBACK, phone).
func erasureFixture(t *testing.T) (*Service, *Store, string) {
	t.Helper()
	svc, store := newTestService(t)
	web := mustAccepted(t, svc, `{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","applicant":{"name":"Ming Li","phone":"+8613800000000"},"consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]}`)
	hotline := mustAccepted(t, svc, `{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`)
	if hotline.RequestID != web.RequestID {
		t.Fatalf("fixture events must merge: %s vs %s", web.RequestID, hotline.RequestID)
	}
	return svc, store, web.RequestID
}

func personJSONOf(t *testing.T, store *Store, requestID string) string {
	t.Helper()
	var personJSON string
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT person_json FROM canonical_requests WHERE request_id = ?`, requestID).Scan(&personJSON); err != nil {
		t.Fatalf("person_json: %v", err)
	}
	return personJSON
}

func countSQL(t *testing.T, store *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := store.db.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func payloadsOf(t *testing.T, store *Store, requestID string) map[int64]string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(),
		`SELECT seq, payload_json FROM events WHERE request_id = ? ORDER BY seq`, requestID)
	if err != nil {
		t.Fatalf("payloads: %v", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var seq int64
		var payload string
		if err := rows.Scan(&seq, &payload); err != nil {
			t.Fatalf("payloads scan: %v", err)
		}
		out[seq] = payload
	}
	return out
}

// assertContactErased verifies every storage layer is free of raw contact
// while non-reversible evidence survives.
func assertContactErased(t *testing.T, svc *Service, store *Store, requestID, erasedBy string) {
	t.Helper()
	if p := personJSONOf(t, store, requestID); strings.Contains(p, revokedPhoneDigits) || strings.Contains(p, "+86") {
		t.Fatalf("person_json still holds raw contact: %s", p)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM request_identities WHERE request_id = ? AND kind IN ('phone','email')`, requestID); n != 0 {
		t.Fatalf("live contact fragments remain: %d", n)
	}
	evidence := countSQL(t, store, `SELECT COUNT(*) FROM erased_identity_evidence WHERE request_id = ? AND kind = 'phone' AND erased_by_event = ?`, requestID, erasedBy)
	if evidence != 1 {
		t.Fatalf("expected 1 phone evidence row by %s, got %d", erasedBy, evidence)
	}
	hashRow := countSQL(t, store, `SELECT COUNT(*) FROM erased_identity_evidence WHERE request_id = ? AND fragment_hash = ?`, requestID, fragmentHash(identityPhone, revokedPhoneDigits))
	if hashRow != 1 {
		t.Fatal("evidence must hold the non-reversible phone hash")
	}
	for seq, payload := range payloadsOf(t, store, requestID) {
		if strings.Contains(payload, revokedPhoneDigits) || strings.Contains(payload, "+86 138") {
			t.Fatalf("payload seq=%d still holds raw contact: %s", seq, payload)
		}
	}
	view, err := svc.GetRequest(context.Background(), requestID)
	if err != nil || view == nil {
		t.Fatalf("view: %v", err)
	}
	if view.Contact != nil {
		t.Fatalf("contact exposed after erasure: %+v", view.Contact)
	}
	audit, _ := svc.GetAudit(context.Background(), requestID)
	if len(audit.ErasedIdentities) == 0 || audit.ErasedIdentities[0].ErasedByEvent != erasedBy {
		t.Fatalf("audit erasedIdentities = %+v", audit.ErasedIdentities)
	}
	if !contains(audit.ConsentRevoked, contactScope) {
		t.Fatalf("audit consentRevoked = %v", audit.ConsentRevoked)
	}
}

// TestFixtureRevocationDoesNotOverClear: the fixture's EV-W-002 revocation
// targets CASE_SUMMARY_TRANSFER — it must not touch contact data at all.
func TestFixtureRevocationDoesNotOverClear(t *testing.T) {
	svc, store, requestID := erasureFixture(t)

	rev := mustAccepted(t, svc, `{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}`)
	if len(rev.AppliedScopes) != 1 || rev.AppliedScopes[0] != "CASE_SUMMARY_TRANSFER" {
		t.Fatalf("appliedScopes = %v", rev.AppliedScopes)
	}
	// Contact scope untouched: no erasure anywhere.
	if p := personJSONOf(t, store, requestID); !strings.Contains(p, revokedPhoneDigits) {
		t.Fatalf("contact must survive a non-contact revocation: %s", p)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM erased_identity_evidence WHERE request_id = ?`, requestID); n != 0 {
		t.Fatalf("non-contact revocation must not create evidence rows: %d", n)
	}
	if n := countSQL(t, store, `SELECT contact_erased FROM canonical_requests WHERE request_id = ?`, requestID); n != 0 {
		t.Fatal("contact_erased flag must stay 0")
	}
	view, _ := svc.GetRequest(context.Background(), requestID)
	if view.Contact == nil || view.Contact.Phone == "" {
		t.Fatalf("contact must remain visible while CONTACT_CALLBACK is effective: %+v", view)
	}
	if contains(view.ConsentEffective, "CASE_SUMMARY_TRANSFER") || !contains(view.ConsentEffective, "ACCOMMODATION_TRANSFER") {
		t.Fatalf("consentEffective = %v", view.ConsentEffective)
	}
	for seq, payload := range payloadsOf(t, store, requestID) {
		if strings.Contains(payload, "REDACTED#") {
			t.Fatalf("non-contact revocation must not redact payloads (seq=%d)", seq)
		}
	}
}

// TestContactRevocationErasesPrecisely: revoking the last CONTACT_CALLBACK
// grant erases exactly the contact channels and keeps everything else.
func TestContactRevocationErasesPrecisely(t *testing.T) {
	svc, store, requestID := erasureFixture(t)
	mustAccepted(t, svc, `{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"]}`)
	mustAccepted(t, svc, `{"eventId":"EV-H-100","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"]}`)

	assertContactErased(t, svc, store, requestID, "EV-H-100")

	view, _ := svc.GetRequest(context.Background(), requestID)
	// Untouched scopes and accommodations survive: no over-clearing.
	if !contains(view.ConsentEffective, "ACCOMMODATION_TRANSFER") {
		t.Fatalf("unrelated scope over-cleared: %v", view.ConsentEffective)
	}
	if view.Person == nil || view.Person.Name == "" {
		t.Fatalf("name (not a contact channel) must survive: %+v", view.Person)
	}
	if view.CallbackWindowMinutes == nil || *view.CallbackWindowMinutes != 45 {
		t.Fatalf("callback window is not contact data: %v", view.CallbackWindowMinutes)
	}
	// payload_hash anchors replay integrity even though payloads are redacted.
	var storedHash string
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT payload_hash FROM events WHERE event_id = 'EV-H-001'`).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	originalHash, _ := PayloadHash([]byte(`{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`))
	if storedHash != originalHash {
		t.Fatal("payload_hash must still cover the original bytes")
	}
}

// TestLateRetryCarryingRevokedPhone: a late hotline retry carrying the
// revoked phone replays the original result without resurrecting anything.
func TestLateRetryCarryingRevokedPhone(t *testing.T) {
	svc, store, requestID := erasureFixture(t)
	mustAccepted(t, svc, `{"eventId":"EV-H-100","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"]}`)
	assertContactErased(t, svc, store, requestID, "EV-H-100")

	hotlineRaw := `{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`
	replay, status := mustProcess(t, svc, hotlineRaw)
	if status != http.StatusOK || !replay.IdempotentReplay {
		t.Fatalf("expected replay, got %d %+v", status, replay)
	}
	// The replayed original result is historical evidence — it may show the
	// then-effective consent — but the live state must stay erased.
	assertContactErased(t, svc, store, requestID, "EV-H-100")
	if n := countSQL(t, store, `SELECT event_count FROM canonical_requests WHERE request_id = ?`, requestID); n != 3 {
		t.Fatalf("replay must not append events: event_count = %d", n)
	}
}

// TestOutOfOrderRevocationRetrySupplement mixes, out of order: a revocation
// arriving before its grant, a transiently-failing grant retry, and a
// physical-channel supplement carrying the revoked phone.
func TestOutOfOrderRevocationRetrySupplement(t *testing.T) {
	svc, store := newTestService(t)
	var mu sync.Mutex
	failedOnce := false
	svc.SetTransientHook(func(eventID string, raw []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if eventID == "EV-H-001" && !failedOnce {
			failedOnce = true
			return errors.New("hotline adapter timeout")
		}
		return nil
	})
	revocation := `{"eventId":"EV-R-1","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"]}`
	hotlineRaw := `{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`
	supplementRaw := `{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","visitor":{"name":"Li Ming","phone":"+86 138 0000 0000"},"accommodations":["BRAILLE_MATERIAL"]}`

	// 1. Revocation before its grant: retryable failure.
	early, status := mustProcess(t, svc, revocation)
	if status != http.StatusUnprocessableEntity || !early.Retryable || early.Errors[0].Code != CodeRevokesUnknownEvent {
		t.Fatalf("early revocation = %d %+v", status, early)
	}
	// 2. Grant attempt fails transiently, rolls back completely.
	if _, status := mustProcess(t, svc, hotlineRaw); status != http.StatusServiceUnavailable {
		t.Fatalf("transient attempt = %d", status)
	}
	// 3. Web record lands (the out-of-order "network registration").
	web := mustAccepted(t, svc, `{"eventId":"EV-W-001","channel":"WEB","submissionId":"W-71","applicant":{"name":"Ming Li","phone":"+8613800000000"},"consent":["CASE_SUMMARY_TRANSFER","ACCOMMODATION_TRANSFER"]}`)
	// 4. Grant retry succeeds, merged via phone evidence.
	hotline := mustAccepted(t, svc, hotlineRaw)
	if hotline.RequestID != web.RequestID || hotline.Match.Decision != MatchMerged {
		t.Fatalf("grant retry = %+v", hotline)
	}
	// 5. Revocation retry now succeeds and erases contact.
	rev := mustAccepted(t, svc, revocation)
	if len(rev.AppliedScopes) != 1 || rev.AppliedScopes[0] != contactScope {
		t.Fatalf("revocation = %+v", rev)
	}
	assertContactErased(t, svc, store, web.RequestID, "EV-R-1")

	// 6. Supplement from another channel carrying the revoked phone: merges
	// via redacted evidence, stores a redacted payload, resurrects nothing.
	supplement := mustAccepted(t, svc, supplementRaw)
	if supplement.RequestID != web.RequestID {
		t.Fatalf("supplement forked a second chain: %s", supplement.RequestID)
	}
	if supplement.Match == nil || supplement.Match.Decision != MatchMerged {
		t.Fatalf("supplement match = %+v", supplement.Match)
	}
	redactedMatch := false
	for _, ev := range supplement.Match.Considered[0].Matched {
		if ev.Kind == identityPhone && ev.Redacted {
			redactedMatch = true
		}
	}
	if !redactedMatch {
		t.Fatalf("expected redacted phone evidence match: %+v", supplement.Match.Considered[0].Matched)
	}
	assertContactErased(t, svc, store, web.RequestID, "EV-R-1")

	// 7. Replays of grant and supplement: original results, no resurrection.
	for _, raw := range []string{hotlineRaw, supplementRaw} {
		if _, status := mustProcess(t, svc, raw); status != http.StatusOK {
			t.Fatalf("replay status = %d", status)
		}
	}
	assertContactErased(t, svc, store, web.RequestID, "EV-R-1")

	// One chain, four events; non-revoked scopes and accommodations intact.
	if n := countSQL(t, store, `SELECT COUNT(*) FROM canonical_requests`); n != 1 {
		t.Fatalf("requests = %d", n)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM events WHERE request_id = ?`, web.RequestID); n != 4 {
		t.Fatalf("events = %d", n)
	}
	view, _ := svc.GetRequest(context.Background(), web.RequestID)
	if !contains(view.ConsentEffective, "ACCOMMODATION_TRANSFER") || !contains(view.ConsentEffective, "CASE_SUMMARY_TRANSFER") {
		t.Fatalf("non-revoked scopes over-cleared: %v", view.ConsentEffective)
	}
	if !contains(view.Accommodations, "BRAILLE_MATERIAL") {
		t.Fatalf("supplement accommodations lost: %v", view.Accommodations)
	}
	// Every attempt is auditable: early revocation failure, transient grant.
	attempts, _ := store.ListAttemptsForRequest(context.Background(), web.RequestID)
	outcomes := map[string]int{}
	for _, a := range attempts {
		outcomes[a.Outcome]++
	}
	if outcomes[AttemptValidationFailed] != 1 || outcomes[AttemptTransientFailure] != 1 || outcomes[AttemptAccepted] != 4 || outcomes[AttemptReplayed] != 2 {
		t.Fatalf("attempt outcomes = %+v", outcomes)
	}
}

// TestRepeatedRevocationIdempotent: replaying the same revocation replays
// its result; a second distinct revocation of an already-erased scope is an
// accepted no-op; evidence is recorded exactly once.
func TestRepeatedRevocationIdempotent(t *testing.T) {
	svc, store, requestID := erasureFixture(t)
	revRaw := `{"eventId":"EV-H-100","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"]}`
	first := mustAccepted(t, svc, revRaw)
	assertContactErased(t, svc, store, requestID, "EV-H-100")

	replay, status := mustProcess(t, svc, revRaw)
	if status != http.StatusOK || !replay.IdempotentReplay {
		t.Fatalf("revocation replay = %d %+v", status, replay)
	}
	replay.IdempotentReplay = false
	wantJSON, _ := json.Marshal(first)
	gotJSON, _ := json.Marshal(replay)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("revocation replay diverged:\nwant %s\ngot  %s", wantJSON, gotJSON)
	}

	second := mustAccepted(t, svc, `{"eventId":"EV-H-101","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"]}`)
	if len(second.AppliedScopes) != 0 {
		t.Fatalf("second revocation must be a no-op: %+v", second)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM erased_identity_evidence WHERE request_id = ?`, requestID); n != 1 {
		t.Fatalf("evidence recorded more than once: %d", n)
	}
	assertContactErased(t, svc, store, requestID, "EV-H-100")
}

// TestConcurrentRevokeAndSupplement: a revocation, a supplement carrying the
// revoked phone, and a grant replay landing concurrently still converge to
// one erased chain — in any transaction order.
func TestConcurrentRevokeAndSupplement(t *testing.T) {
	svc, store := newTestService(t)
	grantRaw := `{"eventId":"EV-G-1","channel":"HOTLINE","callRef":"C-1","callbackWindowMinutes":10,"caller":{"name":"Li Ming","phone":"+86 138 0000 0000"},"consent":["CONTACT_CALLBACK"]}`
	grant := mustAccepted(t, svc, grantRaw)

	raws := []string{
		`{"eventId":"EV-R-9","revokes":"EV-G-1","scopes":["CONTACT_CALLBACK"]}`,
		`{"eventId":"EV-S-9","channel":"WEB","submissionId":"W-9","applicant":{"name":"Ming Li","phone":"+8613800000000"},"accommodations":["TEXT_ONLY"]}`,
		grantRaw, // concurrent replay of the original grant
	}
	results := make([]RecordResult, len(raws))
	var wg sync.WaitGroup
	for i, raw := range raws {
		wg.Add(1)
		go func(i int, raw string) {
			defer wg.Done()
			res, _ := svc.ProcessEnvelope(context.Background(), []byte(raw))
			results[i] = res
		}(i, raw)
	}
	wg.Wait()

	for i, res := range results {
		if res.RequestID != grant.RequestID {
			t.Fatalf("result %d forked the chain: %+v", i, res)
		}
	}
	if results[0].Status != StatusAccepted || results[1].Status != StatusAccepted {
		t.Fatalf("revocation/supplement = %+v %+v", results[0], results[1])
	}
	if !results[2].IdempotentReplay {
		t.Fatalf("concurrent grant replay = %+v", results[2])
	}
	assertContactErased(t, svc, store, grant.RequestID, "EV-R-9")
	if n := countSQL(t, store, `SELECT COUNT(*) FROM events WHERE request_id = ?`, grant.RequestID); n != 3 {
		t.Fatalf("events = %d", n)
	}
	view, _ := svc.GetRequest(context.Background(), grant.RequestID)
	if !contains(view.Accommodations, "TEXT_ONLY") {
		t.Fatalf("supplement accommodations lost: %v", view.Accommodations)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM erased_identity_evidence WHERE request_id = ?`, grant.RequestID); n != 1 {
		t.Fatalf("evidence rows = %d (must be exactly 1 in any order)", n)
	}
}

// TestErasureDoesNotCrossChains: erasure on one chain never touches another,
// including a candidate standalone request sharing the applicant's name.
func TestErasureDoesNotCrossChains(t *testing.T) {
	svc, store, requestA := erasureFixture(t)
	// Chain B: same name, conflicting phone → standalone candidate with its
	// own CONTACT_CALLBACK grant.
	chainB := mustAccepted(t, svc, `{"eventId":"EV-B-1","channel":"HOTLINE","callRef":"C-B","callbackWindowMinutes":20,"caller":{"name":"Li Ming","phone":"+86 139 0000 0000"},"consent":["CONTACT_CALLBACK"]}`)
	if chainB.RequestID == requestA || chainB.Match.Decision != MatchCandidate {
		t.Fatalf("chain B = %+v", chainB)
	}

	mustAccepted(t, svc, `{"eventId":"EV-H-100","revokes":"EV-H-001","scopes":["CONTACT_CALLBACK"]}`)
	assertContactErased(t, svc, store, requestA, "EV-H-100")

	// Chain B is completely untouched (person_json keeps the raw phone).
	if p := personJSONOf(t, store, chainB.RequestID); !strings.Contains(p, "+86 139 0000 0000") {
		t.Fatalf("chain B contact was over-cleared: %s", p)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM request_identities WHERE request_id = ? AND kind = 'phone'`, chainB.RequestID); n != 1 {
		t.Fatalf("chain B fragments = %d", n)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM erased_identity_evidence WHERE request_id = ?`, chainB.RequestID); n != 0 {
		t.Fatalf("chain B evidence = %d", n)
	}
	viewB, _ := svc.GetRequest(context.Background(), chainB.RequestID)
	if viewB.Contact == nil || viewB.Contact.Phone == "" {
		t.Fatalf("chain B contact wrongly hidden: %+v", viewB)
	}
	for seq, payload := range payloadsOf(t, store, chainB.RequestID) {
		if strings.Contains(payload, "REDACTED#") {
			t.Fatalf("chain B payload redacted (seq=%d)", seq)
		}
	}
}

// TestMigrationV1ToV3Upgrade: a v1 database gains identity matching (v2) and
// erasure support (v3); erasure works against pre-v3 records.
func TestMigrationV1ToV3Upgrade(t *testing.T) {
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
		t.Fatalf("migrate v1→v3: %v", err)
	}
	if n := countSQL(t, store, `SELECT COUNT(*) FROM schema_migrations`); n != len(migrations) {
		t.Fatalf("migrations recorded = %d", n)
	}
	// v3 objects exist and are usable.
	if n := countSQL(t, store, `SELECT contact_erased FROM canonical_requests WHERE request_id = 'req_legacy'`); n != 0 {
		t.Fatalf("contact_erased default = %d", n)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO erased_identity_evidence (request_id, kind, fragment_hash, erased_by_event, erased_at) VALUES ('req_legacy','phone','h','EV-X','2026-01-02T00:00:00Z')`); err != nil {
		t.Fatalf("evidence table unusable: %v", err)
	}
}
