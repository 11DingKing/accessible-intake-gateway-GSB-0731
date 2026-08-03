package intake

import "testing"

func cand(rid string, frags ...IdentityFragment) IdentityCandidate {
	return IdentityCandidate{RequestID: rid, Fragments: frags}
}

func TestMatchExactNameAndDOB(t *testing.T) {
	existing := cand("CR-1", IdentityFragment{FirstName: "Ada", LastName: "Lovelace", DateOfBirth: "1815-12-10", Phone: "555-0100"})
	in := IdentityFragment{FirstName: "Ada", LastName: "Lovelace", DateOfBirth: "1815-12-10"}
	dec := MatchFragment(in, []IdentityCandidate{existing})
	if dec.Confidence != ConfidenceExact || dec.Reason != MatchReasonExactIdentity {
		t.Fatalf("expected exact identity match, got %+v", dec)
	}
}

func TestMatchPhoneHighConfidenceMasksConflictingDOB(t *testing.T) {
	existing := cand("CR-2", IdentityFragment{FirstName: "Sam", LastName: "Doe", DateOfBirth: "1980-01-01", Phone: "5551111"})
	in := IdentityFragment{FirstName: "Sam", LastName: "Doe", DateOfBirth: "1980-01-02", Phone: "(555) 11-11"}
	dec := MatchFragment(in, []IdentityCandidate{existing})
	if !dec.AutoMerge() {
		t.Fatalf("phone match should auto-merge: %+v", dec)
	}
	if !dec.Conflict {
		t.Fatalf("different DOB with same name+phone must be conflict evidence: %+v", dec)
	}
	for _, e := range dec.Evidence {
		if e.Field == "dateOfBirth" && e.Matched == false && e.Value != "" {
			t.Fatalf("conflicting DOB value must not be exposed in evidence: %+v", e)
		}
	}
}

func TestMatchDeterministicTieBreakByRequestID(t *testing.T) {
	c1 := cand("CR-B", IdentityFragment{Phone: "5552222"})
	c2 := cand("CR-A", IdentityFragment{Phone: "5552222"})
	in := IdentityFragment{Phone: "555-2222"}
	dec := MatchFragment(in, []IdentityCandidate{c1, c2})
	if dec.RequestID != "CR-A" {
		t.Fatalf("deterministic tie-break should pick lowest request ID, got %s", dec.RequestID)
	}
}

func TestNoMatchForUnrelatedFragment(t *testing.T) {
	existing := cand("CR-3", IdentityFragment{FirstName: "X", LastName: "Y", DateOfBirth: "1990-01-01"})
	in := IdentityFragment{FirstName: "A", LastName: "B", DateOfBirth: "1999-09-09"}
	dec := MatchFragment(in, []IdentityCandidate{existing})
	if dec.AutoMerge() {
		t.Fatalf("unrelated fragment must not auto-merge: %+v", dec)
	}
}
