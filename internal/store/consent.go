package store

import (
	"context"
)

// HasConsentScope reports whether scope is currently granted on the chain
// (latest action per scope is GRANT). It reads within the current transaction
// so callers see uncommitted state when invoked mid-write.
func (t *Tx) HasConsentScope(ctx context.Context, canonicalID, scope string) (bool, error) {
	var action string
	err := t.tx.QueryRowContext(ctx,
		`SELECT action FROM consent_records
		 WHERE canonical_request_id=? AND scope=?
		 ORDER BY sequence DESC, id DESC LIMIT 1`,
		canonicalID, scope,
	).Scan(&action)
	if err != nil {
		// No rows means not granted.
		return false, nil
	}
	return action == "GRANT", nil
}

// HasConsentScopeReadOnly is the read-only variant.
func (s *Store) HasConsentScope(ctx context.Context, canonicalID, scope string) (bool, error) {
	var action string
	err := s.db.QueryRowContext(ctx,
		`SELECT action FROM consent_records
		 WHERE canonical_request_id=? AND scope=?
		 ORDER BY sequence DESC, id DESC LIMIT 1`,
		canonicalID, scope,
	).Scan(&action)
	if err != nil {
		return false, nil
	}
	return action == "GRANT", nil
}
