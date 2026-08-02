package identity

import "testing"

func TestNormalizePhone(t *testing.T) {
	if got := Normalize(FragPhone, " +1 (415) 555-0100 "); got != "14155550100" {
		t.Fatalf("phone normalize = %q", got)
	}
}

func TestHashStableAndDomainSeparated(t *testing.T) {
	if Hash(FragEmail, "A@B.com") != Hash(FragEmail, "a@b.com") {
		t.Fatalf("email hash should be case-insensitive")
	}
	// Same value under different fragment types must not collide.
	if Hash(FragEmail, "x") == Hash(FragPhone, "x") {
		t.Fatalf("hash must be domain-separated by fragment type")
	}
}

func TestClassifyWindow(t *testing.T) {
	cases := map[int]string{
		0:    WindowImmediate,
		1:    WindowIntraday,
		1439: WindowIntraday,
		1440: WindowCrossDay,
		5000: WindowCrossDay,
	}
	for min, want := range cases {
		if got := ClassifyWindow(min); got != want {
			t.Fatalf("ClassifyWindow(%d) = %q, want %q", min, got, want)
		}
	}
}

func TestScoreConfidence(t *testing.T) {
	if c, _ := Score([]string{FragCorrelation}); c != ConfidenceHigh {
		t.Fatalf("correlation should be HIGH, got %s", c)
	}
	if c, _ := Score([]string{FragPhone, FragEmail}); c != ConfidenceHigh {
		t.Fatalf("phone+email (1.0) should be HIGH, got %s", c)
	}
	if c, _ := Score([]string{FragPhone}); c != ConfidenceMedium {
		t.Fatalf("phone alone (0.5) should be MEDIUM, got %s", c)
	}
	if c, _ := Score([]string{FragPostalCode}); c != ConfidenceLow {
		t.Fatalf("postal alone (0.15) should be LOW, got %s", c)
	}
	if c, _ := Score(nil); c != ConfidenceNone {
		t.Fatalf("no matches should be NONE, got %s", c)
	}
}
