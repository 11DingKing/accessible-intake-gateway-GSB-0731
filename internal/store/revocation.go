package store

import (
	"context"
	"time"
)

// RevokeField records that a reversible field has been cleared for a chain.
// It is idempotent: revoking an already-revoked field is a no-op (INSERT OR
// IGNORE) so duplicate revocation events do not duplicate records or change
// the revoked_at timestamp.
func (t *Tx) RevokeField(ctx context.Context, canonicalID, field, revokedBy string) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO revoked_fields(canonical_request_id, field, revoked_by_event, revoked_at)
		 VALUES(?,?,?,?)`,
		canonicalID, field, revokedBy, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// RevokedFields returns the set of field names currently revoked for a chain.
func (t *Tx) RevokedFields(ctx context.Context, canonicalID string) (map[string]bool, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT field FROM revoked_fields WHERE canonical_request_id=?`, canonicalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		out[f] = true
	}
	return out, rows.Err()
}

// RevokedFieldsReadOnly is the read-only variant.
func (s *Store) RevokedFields(ctx context.Context, canonicalID string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT field FROM revoked_fields WHERE canonical_request_id=?`, canonicalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		out[f] = true
	}
	return out, rows.Err()
}

// ClearCanonicalField sets a scalar PII column to '' on the canonical header.
func (t *Tx) ClearCanonicalField(ctx context.Context, canonicalID, column string) error {
	switch column {
	case "phone":
		_, err := t.tx.ExecContext(ctx,
			`UPDATE canonical_requests SET phone='', phone_digits='' WHERE id=?`, canonicalID)
		return err
	case "email":
		_, err := t.tx.ExecContext(ctx,
			`UPDATE canonical_requests SET email='' WHERE id=?`, canonicalID)
		return err
	case "full_name":
		_, err := t.tx.ExecContext(ctx,
			`UPDATE canonical_requests SET full_name='' WHERE id=?`, canonicalID)
		return err
	default:
		return nil
	}
}

// RedactEventField clears a scalar PII column on EVERY event row in the chain.
// The event's payload hash, channel, source id, sequence, and timestamps are
// preserved as non-reversible evidence.
func (t *Tx) RedactEventField(ctx context.Context, canonicalID, column string) error {
	var col string
	switch column {
	case "phone":
		col = "person_phone"
	case "email":
		col = "person_email"
	case "full_name":
		col = "person_name"
	default:
		return nil
	}
	_, err := t.tx.ExecContext(ctx,
		`UPDATE events SET `+col+`='' WHERE canonical_request_id=? AND is_revocation=0`,
		canonicalID)
	return err
}

// DeleteAccommodations removes all accommodation records for the chain (called
// when ACCOMMODATION_TRANSFER is revoked). The event evidence remains.
func (t *Tx) DeleteAccommodations(ctx context.Context, canonicalID string) error {
	_, err := t.tx.ExecContext(ctx,
		`DELETE FROM accommodation_records WHERE canonical_request_id=?`, canonicalID)
	return err
}

// IsFieldRevoked reports whether a field is in the revoked set.
func IsFieldRevoked(revoked map[string]bool, field string) bool {
	if revoked == nil {
		return false
	}
	return revoked[field]
}
