package gateway

import (
	"context"
	"path/filepath"
	"testing"
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	reg, err := LoadRegistry(filepath.Join("..", "..", "materials", "channel-contracts.json"))
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	return reg
}

func openMemoryStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open("file::memory:?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestService(t *testing.T) (*Service, *Store) {
	t.Helper()
	store := openMemoryStore(t)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewService(store, testRegistry(t)), store
}

// TestMigrateIsRepeatable proves the database can be created repeatedly:
// migrate twice on one handle, then reopen the same file and migrate again.
func TestMigrateIsRepeatable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intake.db")
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_txlock=immediate"

	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	var versionCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&versionCount); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if versionCount != len(migrations) {
		t.Fatalf("expected %d recorded migrations, got %d", len(migrations), versionCount)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if err := reopened.Migrate(ctx); err != nil {
		t.Fatalf("migrate after reopen: %v", err)
	}

	for _, table := range []string{"canonical_requests", "events", "attempts"} {
		var name string
		if err := reopened.db.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("table %s missing: %v", table, err)
		}
	}
}

// TestRegistryLoadsContractFixture pins the fixture values so channel IDs,
// scopes, codes and the minute-based callback field are never renamed.
func TestRegistryLoadsContractFixture(t *testing.T) {
	reg := testRegistry(t)
	if reg.CanonicalVersion != "intake.v1" {
		t.Fatalf("canonicalVersion = %q", reg.CanonicalVersion)
	}
	for _, ch := range []string{"PHYSICAL", "HOTLINE", "WEB"} {
		if _, ok := reg.Channel(ch); !ok {
			t.Fatalf("channel %s missing", ch)
		}
	}
	phys, _ := reg.Channel("PHYSICAL")
	if phys.SourceIDField != "deskReceiptNo" || phys.PersonField != "visitor" {
		t.Fatalf("PHYSICAL contract = %+v", phys)
	}
	hot, _ := reg.Channel("HOTLINE")
	if hot.SourceIDField != "callRef" || hot.PersonField != "caller" || hot.CallbackWindowField != "callbackWindowMinutes" {
		t.Fatalf("HOTLINE contract = %+v", hot)
	}
	web, _ := reg.Channel("WEB")
	if web.SourceIDField != "submissionId" || web.PersonField != "applicant" {
		t.Fatalf("WEB contract = %+v", web)
	}
	for _, code := range []string{"BRAILLE_MATERIAL", "SIGN_INTERPRETER", "TEXT_ONLY", "STEP_FREE_ACCESS"} {
		if !reg.KnownAccommodation(code) {
			t.Fatalf("accommodation %s missing", code)
		}
	}
	for _, scope := range []string{"CONTACT_CALLBACK", "CASE_SUMMARY_TRANSFER", "ACCOMMODATION_TRANSFER"} {
		if !reg.KnownConsentScope(scope) {
			t.Fatalf("consent scope %s missing", scope)
		}
	}
}
