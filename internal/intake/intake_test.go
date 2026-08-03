package intake

import (
	"testing"
)

func TestProjectionHashDeterministicAndRevocation(t *testing.T) {
	phone := "5551234"
	cb := 30
	events := []*AppliedEvent{
		{EventID: "A", Channel: "WEB", SourceID: "W1", Person: Person{FirstName: "A", LastName: "B", DateOfBirth: "1990-01-01", Phone: phone}, Consent: []string{"CONTACT_CALLBACK"}, CallbackWindowMinutes: &cb},
		{EventID: "B", Channel: "PHYSICAL", SourceID: "D1", Accommodations: []string{"TEXT_ONLY"}, Consent: []string{"ACCOMMODATION_TRANSFER"}},
	}
	v1 := Project("CR1", events)
	v2 := Project("CR1", events)
	if v1.ProjectionHash != v2.ProjectionHash {
		t.Fatalf("projection must be deterministic")
	}
	if !v1.ContactAvailable || v1.Person.Phone != phone {
		t.Fatalf("contact should be available before revocation")
	}
	if len(v1.Accommodations) != 1 {
		t.Fatalf("accommodation transfer should expose TEXT_ONLY")
	}

	rev := &AppliedEvent{EventID: "R", RevokesEventID: "A", RevocationScopes: []string{"CONTACT_CALLBACK"}}
	v3 := Project("CR1", append(events, rev))
	if v3.ContactAvailable {
		t.Fatalf("contact must be revoked")
	}
	if v3.Person.Phone != "" {
		t.Fatalf("phone must be redacted, got %q", v3.Person.Phone)
	}
	if v3.CallbackWindowMinutes != nil {
		t.Fatalf("callback window must be hidden when contact consent is revoked")
	}
}

func TestOutOfOrderRevocationIsPlacedAfterGrant(t *testing.T) {
	grant := &AppliedEvent{EventID: "G", Channel: "WEB", SourceID: "W", Consent: []string{"CONTACT_CALLBACK"}, Person: Person{FirstName: "X", LastName: "Y", DateOfBirth: "1980-01-01", Phone: "123"}}
	rev := &AppliedEvent{EventID: "R", RevokesEventID: "G", RevocationScopes: []string{"CONTACT_CALLBACK"}, ArrivalOrder: 1}
	grant.ArrivalOrder = 2
	// revocation arrives first in DB sequence
	ordered := projectionOrder([]*AppliedEvent{rev, grant})
	if ordered[0].EventID != "G" || ordered[1].EventID != "R" {
		t.Fatalf("expected grant before revocation, got %s then %s", ordered[0].EventID, ordered[1].EventID)
	}
	v := Project("CR", []*AppliedEvent{rev, grant})
	if v.ContactAvailable {
		t.Fatalf("revocation after grant must remove contact")
	}
}
