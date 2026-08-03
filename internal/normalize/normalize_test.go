package normalize

import (
	"testing"

	"github.com/accessible-intake-gateway/internal/contract"
	"github.com/accessible-intake-gateway/internal/model"
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

func testContract(t *testing.T) *contract.Contract {
	t.Helper()
	c, err := contract.Parse([]byte(fixture))
	if err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	return c
}

func TestNormalizePhysical(t *testing.T) {
	c := testContract(t)
	ev, errs := Normalize(c, []byte(`{"eventId":"EV-P-001","channel":"PHYSICAL","deskReceiptNo":"D-88","accommodations":["BRAILLE_MATERIAL"],"canonicalKey":"APP-1"}`))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %+v", errs)
	}
	if ev.SourceIDField != "deskReceiptNo" || ev.SourceID != "D-88" {
		t.Fatalf("source field not preserved: %+v", ev)
	}
	if ev.CanonicalKey != "APP-1" {
		t.Fatalf("canonical key = %q", ev.CanonicalKey)
	}
}

func TestNormalizeHotlineCallbackWindow(t *testing.T) {
	c := testContract(t)
	ev, errs := Normalize(c, []byte(`{"eventId":"EV-H-001","channel":"HOTLINE","callRef":"C-19","callbackWindowMinutes":45,"consent":["CONTACT_CALLBACK"]}`))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %+v", errs)
	}
	if ev.CallbackWindowMinutes == nil || *ev.CallbackWindowMinutes != 45 {
		t.Fatalf("callback window not preserved: %+v", ev.CallbackWindowMinutes)
	}
}

func TestNormalizeNegativeCallbackWindow(t *testing.T) {
	c := testContract(t)
	_, errs := Normalize(c, []byte(`{"eventId":"EV-H-002","channel":"HOTLINE","callRef":"C-20","callbackWindowMinutes":-5}`))
	if !hasCode(errs, CodeInvalidValue) {
		t.Fatalf("expected invalid value error, got %+v", errs)
	}
}

func TestNormalizeUnknownAccommodation(t *testing.T) {
	c := testContract(t)
	_, errs := Normalize(c, []byte(`{"eventId":"EV-P-9","channel":"PHYSICAL","deskReceiptNo":"D-9","accommodations":["ROBOT_HELPER"]}`))
	if !hasCode(errs, CodeUnknownValue) {
		t.Fatalf("expected unknown value error, got %+v", errs)
	}
}

func TestNormalizeUnknownChannel(t *testing.T) {
	c := testContract(t)
	_, errs := Normalize(c, []byte(`{"eventId":"EV-X","channel":"CARRIER_PIGEON","ref":"x"}`))
	if !hasCode(errs, CodeUnknownChannel) {
		t.Fatalf("expected unknown channel error, got %+v", errs)
	}
}

func TestNormalizeMissingFields(t *testing.T) {
	c := testContract(t)
	_, errs := Normalize(c, []byte(`{"channel":"WEB"}`))
	if !hasCode(errs, CodeMissingField) {
		t.Fatalf("expected missing field error, got %+v", errs)
	}
}

func TestNormalizeRevocation(t *testing.T) {
	c := testContract(t)
	ev, errs := Normalize(c, []byte(`{"eventId":"EV-W-002","revokes":"EV-W-001","scopes":["CASE_SUMMARY_TRANSFER"],"canonicalKey":"APP-1"}`))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %+v", errs)
	}
	if ev.Kind != "REVOCATION" || ev.RevokesEventID != "EV-W-001" {
		t.Fatalf("revocation not parsed: %+v", ev)
	}
}

func TestPayloadHashStableAcrossKeyOrder(t *testing.T) {
	c := testContract(t)
	a, _ := Normalize(c, []byte(`{"eventId":"EV-1","channel":"WEB","submissionId":"W-1"}`))
	b, _ := Normalize(c, []byte(`{"channel":"WEB","submissionId":"W-1","eventId":"EV-1"}`))
	if a.PayloadHash != b.PayloadHash {
		t.Fatalf("hash not stable across key order: %s vs %s", a.PayloadHash, b.PayloadHash)
	}
}

func TestPayloadHashDiffersOnValueChange(t *testing.T) {
	c := testContract(t)
	a, _ := Normalize(c, []byte(`{"eventId":"EV-1","channel":"WEB","submissionId":"W-1"}`))
	b, _ := Normalize(c, []byte(`{"eventId":"EV-1","channel":"WEB","submissionId":"W-2"}`))
	if a.PayloadHash == b.PayloadHash {
		t.Fatalf("hash should differ on payload change")
	}
}

func hasCode(errs []model.RecordError, code string) bool {
	for _, e := range errs {
		if e.Code == code {
			return true
		}
	}
	return false
}
