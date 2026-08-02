// Package store provides SQLite persistence for the intake gateway. The schema
// is created idempotently on Open so databases are repeatable across runs.
package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a *sql.DB pool.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at dsn and ensures the schema
// exists. The DSN is opened with WAL, foreign keys, and a busy timeout so
// concurrent writers serialize safely.
func Open(dsn string) (*Store, error) {
	full := dsn + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	db, err := sql.Open("sqlite", full)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite writes serialize; one connection avoids lock churn.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the raw pool for specialized callers (tests, migrations).
func (s *Store) DB() *sql.DB { return s.db }

const schema = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_requests (
  id              TEXT PRIMARY KEY,
  subject_key     TEXT NOT NULL UNIQUE,
  canonical_version TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  full_name       TEXT NOT NULL DEFAULT '',
  phone           TEXT NOT NULL DEFAULT '',
  email           TEXT NOT NULL DEFAULT '',
  person_ref      TEXT NOT NULL DEFAULT '',
  callback_window_minutes INTEGER,
  sequence        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
  event_id            TEXT PRIMARY KEY,
  canonical_request_id TEXT NOT NULL REFERENCES canonical_requests(id),
  channel             TEXT NOT NULL,
  source_id           TEXT NOT NULL DEFAULT '',
  source_id_field     TEXT NOT NULL DEFAULT '',
  payload_hash        TEXT NOT NULL,
  payload_json        TEXT NOT NULL,
  is_revocation       INTEGER NOT NULL DEFAULT 0,
  revokes             TEXT NOT NULL DEFAULT '',
  person_name         TEXT NOT NULL DEFAULT '',
  person_phone        TEXT NOT NULL DEFAULT '',
  person_email        TEXT NOT NULL DEFAULT '',
  person_ref          TEXT NOT NULL DEFAULT '',
  callback_window_minutes INTEGER,
  sequence            INTEGER NOT NULL,
  status              TEXT NOT NULL,
  item_results_json   TEXT NOT NULL DEFAULT '[]',
  created_at          TEXT NOT NULL,
  UNIQUE(canonical_request_id, sequence)
);

CREATE TABLE IF NOT EXISTS event_attempts (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id     TEXT NOT NULL,
  attempt_no   INTEGER NOT NULL,
  request_hash TEXT NOT NULL,
  status       TEXT NOT NULL,
  errors_json  TEXT NOT NULL DEFAULT '[]',
  created_at   TEXT NOT NULL,
  UNIQUE(event_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS consent_records (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_request_id TEXT NOT NULL REFERENCES canonical_requests(id),
  event_id             TEXT NOT NULL,
  sequence             INTEGER NOT NULL,
  scope                TEXT NOT NULL,
  action               TEXT NOT NULL, -- GRANT or REVOKE
  created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accommodation_records (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_request_id TEXT NOT NULL REFERENCES canonical_requests(id),
  event_id             TEXT NOT NULL,
  sequence             INTEGER NOT NULL,
  code                 TEXT NOT NULL,
  created_at           TEXT NOT NULL,
  UNIQUE(canonical_request_id, code)
);

CREATE TABLE IF NOT EXISTS validation_errors (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id     TEXT NOT NULL,
  attempt_id   INTEGER,
  item         TEXT NOT NULL,
  code         TEXT NOT NULL,
  message      TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_canonical ON events(canonical_request_id, sequence);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON event_attempts(event_id, attempt_no);
CREATE INDEX IF NOT EXISTS idx_consent_canonical ON consent_records(canonical_request_id, sequence);
CREATE INDEX IF NOT EXISTS idx_accom_canonical ON accommodation_records(canonical_request_id);
`

func (s *Store) migrate() error {
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	// Idempotent column additions for databases created before a column was
	// introduced. Ignore "duplicate column" errors.
	alterations := []string{
		`ALTER TABLE canonical_requests ADD COLUMN person_ref TEXT NOT NULL DEFAULT ''`,
	}
	for _, a := range alterations {
		if _, err := s.db.Exec(a); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
				return fmt.Errorf("migration %q: %w", a, err)
			}
		}
	}
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?)`,
		1, time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	return nil
}
