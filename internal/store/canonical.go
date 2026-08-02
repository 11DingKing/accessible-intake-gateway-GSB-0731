package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/accessible-intake-gateway/internal/identity"
	"github.com/accessible-intake-gateway/internal/model"
)

// applyCanonical converges one valid event into its canonical request within
// the current transaction. It resolves the event to exactly one request
// (creating one only when nothing matches), appends to the event chain exactly
// once, merges accommodations, updates the consent ledger with sticky-revocation
// semantics, and records deterministic match evidence.
func applyCanonical(tx *sql.Tx, now string, attemptNo int, ev *model.CanonicalEvent) (model.RecordResult, error) {
	res := model.RecordResult{EventID: ev.EventID, Channel: ev.Channel, AttemptNo: attemptNo}

	requestID, key, created, dec, err := resolveEvent(tx, ev)
	if err != nil {
		return res, err
	}
	res.CanonicalKey = key
	// Bind the event's canonical key to the resolved request so downstream
	// projections and audit entries agree on one identity.
	ev.CanonicalKey = key
	d := dec
	res.Match = &d

	// Append to the event chain. The UNIQUE(request_id, event_id) constraint
	// makes concurrent submissions converge to a single chain: the second
	// insert of the same event is ignored, so no duplicate link is created.
	linkRes, err := tx.Exec(`INSERT OR IGNORE INTO event_link(request_id, event_id, channel, kind, source_id_field, source_id, payload_hash) VALUES(?,?,?,?,?,?,?)`,
		requestID, ev.EventID, ev.Channel, string(ev.Kind), nullStr(ev.SourceIDField), nullStr(ev.SourceID), ev.PayloadHash)
	if err != nil {
		return res, fmt.Errorf("append event link: %w", err)
	}
	seq, _ := currentSeq(tx)

	// Record deterministic match evidence (no raw contact values).
	if err := recordMatchEvidence(tx, now, ev.EventID, requestID, dec); err != nil {
		return res, err
	}

	// Merge accommodations (set semantics).
	for _, code := range ev.Accommodations {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO accommodation(request_id, code) VALUES(?,?)`, requestID, code); err != nil {
			return res, fmt.Errorf("merge accommodation: %w", err)
		}
	}

	// Consent grants. A grant never resurrects a scope that is already REVOKED:
	// the ledger only transitions GRANTED for scopes that are absent or already
	// granted, so replays and out-of-order grants cannot re-expose withdrawn
	// consent.
	for _, scope := range ev.ConsentGrants {
		if err := grantConsent(tx, requestID, scope, seq); err != nil {
			return res, err
		}
	}

	// Consent revocations are sticky and terminal for the scope.
	for _, scope := range ev.ConsentRevokes {
		if err := revokeConsent(tx, requestID, scope, seq); err != nil {
			return res, err
		}
	}

	// Persist the callback window/disposition and any raw contact methods. Raw
	// contact values are stored but only ever surface through the consent gate;
	// if a governing scope is already revoked, the raw value is suppressed here
	// so a late/retried event cannot resurrect it.
	suppressed, err := projectContact(tx, requestID, ev)
	if err != nil {
		return res, err
	}

	// For every scope revoked by this event, erase only the raw/reversible data
	// that scope governs, preserving non-reversible evidence. Idempotent.
	erased := 0
	for _, scope := range ev.ConsentRevokes {
		n, err := eraseScopeData(tx, requestID, scope)
		if err != nil {
			return res, err
		}
		erased += n
	}

	if _, err := tx.Exec(`UPDATE canonical_request SET updated_seq = ? WHERE id = ?`, seq, requestID); err != nil {
		return res, fmt.Errorf("bump updated_seq: %w", err)
	}

	action := "EVENT_APPENDED"
	detail := fmt.Sprintf("channel=%s kind=%s match=%s conf=%s", ev.Channel, ev.Kind, dec.Reason, dec.Confidence)
	if n, _ := linkRes.RowsAffected(); n == 0 {
		action = "EVENT_CONVERGED"
		detail += " (chain already contained this event)"
	}
	if created {
		if err := writeAudit(tx, now, key, ev.EventID, "REQUEST_CREATED", fmt.Sprintf("version=%s", key)); err != nil {
			return res, err
		}
	}
	if err := writeAudit(tx, now, key, ev.EventID, action, detail); err != nil {
		return res, err
	}
	if len(dec.ConflictingOn) > 0 {
		if err := writeAudit(tx, now, key, ev.EventID, "IDENTITY_CONFLICT", "conflictingOn="+joinSorted(dec.ConflictingOn)); err != nil {
			return res, err
		}
	}
	if len(ev.ConsentRevokes) > 0 {
		if err := writeAudit(tx, now, key, ev.EventID, "CONSENT_REVOKED", joinSorted(ev.ConsentRevokes)); err != nil {
			return res, err
		}
		// Record a non-reversible erasure summary (counts only, no raw values).
		if err := writeAudit(tx, now, key, ev.EventID, "CONTACT_ERASED",
			fmt.Sprintf("scopes=%s clearedItems=%d", joinSorted(ev.ConsentRevokes), erased)); err != nil {
			return res, err
		}
	}
	if suppressed {
		// A late/retried event carried data governed by an already-revoked scope;
		// it was withheld rather than persisted.
		if err := writeAudit(tx, now, key, ev.EventID, "CONTACT_SUPPRESSED", "governing scope revoked; raw contact withheld"); err != nil {
			return res, err
		}
	}

	res.Status = model.StatusAccepted
	return res, nil
}

// resolveEvent binds an event to a canonical request. Revocations resolve by
// their target correlation number (via the alias index) so a withdrawal lands
// on the same request it references; standard events use full identity
// resolution.
func resolveEvent(tx *sql.Tx, ev *model.CanonicalEvent) (int64, string, bool, model.MatchDecision, error) {
	if ev.Kind == model.KindRevocation {
		aliasKey := ev.CanonicalKey
		if aliasKey == "" {
			aliasKey = ev.RevokesEventID
		}
		// A revocation must land on the SAME converged request it withdraws
		// consent from. Resolve through the alias index (a correlation number may
		// be an alias of a fragment-keyed request) and, failing that, through the
		// request that owns the referenced event. Only create a new request when
		// neither exists, so a revocation never forks the chain.
		if reqID, key, ok, err := revocationTarget(tx, aliasKey, ev.RevokesEventID); err != nil {
			return 0, "", false, model.MatchDecision{}, err
		} else if ok {
			if err := attachAlias(tx, aliasKey, reqID); err != nil {
				return 0, "", false, model.MatchDecision{}, err
			}
			return reqID, key, false, decision(key, "REVOCATION_TARGET", nil, nil, false), nil
		}
		reqID, key, created, err := createOrGetByKey(tx, aliasKey)
		if err != nil {
			return 0, "", false, model.MatchDecision{}, err
		}
		return reqID, key, created, decision(key, "REVOCATION_TARGET", nil, nil, created), nil
	}
	return resolve(tx, ev)
}

// revocationTarget finds the request a revocation should apply to: first by the
// alias/canonical key, then by the request that owns the referenced event id.
func revocationTarget(tx *sql.Tx, aliasKey, revokesEventID string) (int64, string, bool, error) {
	if aliasKey != "" {
		var reqID int64
		err := tx.QueryRow(`SELECT cr.id FROM canonical_request cr
			WHERE cr.canonical_key = ?
			   OR cr.id = (SELECT request_id FROM request_alias WHERE alias_key = ?)`, aliasKey, aliasKey).Scan(&reqID)
		if err == nil {
			key, kerr := canonicalKeyOf(tx, reqID)
			return reqID, key, true, kerr
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, "", false, fmt.Errorf("revocation alias lookup: %w", err)
		}
	}
	if revokesEventID != "" {
		var reqID int64
		err := tx.QueryRow(`SELECT request_id FROM event_link WHERE event_id = ? LIMIT 1`, revokesEventID).Scan(&reqID)
		if err == nil {
			key, kerr := canonicalKeyOf(tx, reqID)
			return reqID, key, true, kerr
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, "", false, fmt.Errorf("revocation event lookup: %w", err)
		}
	}
	return 0, "", false, nil
}

// recordMatchEvidence persists the deterministic match reason and confidence
// evidence for an event. It is idempotent per event id.
func recordMatchEvidence(tx *sql.Tx, now, eventID string, requestID int64, dec model.MatchDecision) error {
	_, err := tx.Exec(`INSERT OR IGNORE INTO match_evidence(event_id, request_id, canonical_key, reason, confidence, score, matched_on, conflicting_on, new_request, at)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		eventID, requestID, dec.CanonicalKey, dec.Reason, dec.Confidence, dec.Score,
		nullStr(joinSorted(dec.MatchedOn)), nullStr(joinSorted(dec.ConflictingOn)), boolInt(dec.NewRequest), now)
	if err != nil {
		return fmt.Errorf("record match evidence: %w", err)
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// currentSeq returns a monotonically increasing sequence sourced from the
// event_link autoincrement counter, giving a stable ordering for projections.
func currentSeq(tx *sql.Tx) (int64, error) {
	var seq sql.NullInt64
	if err := tx.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='event_link'`).Scan(&seq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return seq.Int64, nil
}

func grantConsent(tx *sql.Tx, requestID int64, scope string, seq int64) error {
	var state string
	err := tx.QueryRow(`SELECT state FROM consent WHERE request_id = ? AND scope = ?`, requestID, scope).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec(`INSERT INTO consent(request_id, scope, state, granted_at_seq) VALUES(?,?, 'GRANTED', ?)`, requestID, scope, seq)
		if err != nil {
			return fmt.Errorf("grant consent: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load consent: %w", err)
	}
	// If already REVOKED, do nothing: a grant must not re-expose withdrawn consent.
	return nil
}

func revokeConsent(tx *sql.Tx, requestID int64, scope string, seq int64) error {
	// Upsert into REVOKED state. Once revoked it stays revoked.
	_, err := tx.Exec(`INSERT INTO consent(request_id, scope, state, revoked_at_seq) VALUES(?,?, 'REVOKED', ?)
		ON CONFLICT(request_id, scope) DO UPDATE SET state='REVOKED', revoked_at_seq=excluded.revoked_at_seq`,
		requestID, scope, seq)
	if err != nil {
		return fmt.Errorf("revoke consent: %w", err)
	}
	return nil
}

// scopeState returns the current consent state for a scope ("GRANTED",
// "REVOKED") and whether a row exists.
func scopeState(tx *sql.Tx, requestID int64, scope string) (string, bool, error) {
	var state string
	err := tx.QueryRow(`SELECT state FROM consent WHERE request_id = ? AND scope = ?`, requestID, scope).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load scope state: %w", err)
	}
	return state, true, nil
}

func scopeRevoked(tx *sql.Tx, requestID int64, scope string) (bool, error) {
	state, ok, err := scopeState(tx, requestID, scope)
	if err != nil {
		return false, err
	}
	return ok && state == "REVOKED", nil
}

// contactMethodsGovernedBy lists the raw contact-method kinds whose exposure is
// governed by a consent scope. Only CONTACT_CALLBACK governs raw phone/email
// and the exact (reversible) callback window; other scopes govern no raw
// contact data, so revoking them never touches contact info.
func contactMethodsGovernedBy(scope string) []string {
	switch scope {
	case "CONTACT_CALLBACK":
		return []string{identity.FragPhone, identity.FragEmail}
	default:
		return nil
	}
}

// eraseScopeData removes only the original contact values / reversible digests
// governed by a revoked scope, while preserving legitimate, non-reversible
// event evidence (identity-fragment hashes, the coarse callback disposition,
// match evidence, and the event chain). It is idempotent: a repeated revocation
// erases nothing further. It returns the number of raw items cleared, for audit.
func eraseScopeData(tx *sql.Tx, requestID int64, scope string) (int, error) {
	methods := contactMethodsGovernedBy(scope)
	if len(methods) == 0 {
		return 0, nil
	}
	cleared := 0
	for _, m := range methods {
		r, err := tx.Exec(`DELETE FROM contact_method WHERE request_id = ? AND method = ?`, requestID, m)
		if err != nil {
			return 0, fmt.Errorf("erase contact method: %w", err)
		}
		if n, _ := r.RowsAffected(); n > 0 {
			cleared += int(n)
		}
	}
	// Null out the exact (reversible) callback window minutes but KEEP the coarse
	// disposition bucket, which is non-reversible legitimate evidence.
	r, err := tx.Exec(`UPDATE contact_detail SET callback_window_minutes = NULL
		WHERE request_id = ? AND callback_window_minutes IS NOT NULL`, requestID)
	if err != nil {
		return 0, fmt.Errorf("erase callback window: %w", err)
	}
	if n, _ := r.RowsAffected(); n > 0 {
		cleared += int(n)
	}
	return cleared, nil
}

// projectContact persists the callback window, its disposition, and any raw
// contact methods. Raw values are stored for audit continuity but are only ever
// emitted through the consent gate in GetCanonical. Crucially, if the scope
// that governs a piece of raw contact data is already REVOKED, that raw value
// is NOT re-stored — a late-arriving or retried event carrying revoked contact
// info cannot resurrect it. The coarse callback disposition (non-reversible
// evidence) is always recorded. It returns true if any raw value was suppressed
// because its governing scope was revoked.
func projectContact(tx *sql.Tx, requestID int64, ev *model.CanonicalEvent) (bool, error) {
	callbackRevoked, err := scopeRevoked(tx, requestID, "CONTACT_CALLBACK")
	if err != nil {
		return false, err
	}
	suppressed := false

	if ev.CallbackWindowMinutes != nil {
		disp := identity.ClassifyWindow(*ev.CallbackWindowMinutes)
		if callbackRevoked {
			// Keep only the non-reversible disposition; never persist the exact
			// (reversible) minute value against a revoked scope.
			if _, err := tx.Exec(`INSERT INTO contact_detail(request_id, callback_disposition) VALUES(?,?)
				ON CONFLICT(request_id) DO UPDATE SET callback_disposition=excluded.callback_disposition`,
				requestID, disp); err != nil {
				return false, fmt.Errorf("store contact disposition: %w", err)
			}
			suppressed = true
		} else {
			if _, err := tx.Exec(`INSERT INTO contact_detail(request_id, callback_window_minutes, callback_disposition) VALUES(?,?,?)
				ON CONFLICT(request_id) DO UPDATE SET callback_window_minutes=excluded.callback_window_minutes, callback_disposition=excluded.callback_disposition`,
				requestID, *ev.CallbackWindowMinutes, disp); err != nil {
				return false, fmt.Errorf("store contact detail: %w", err)
			}
		}
	}

	for method, value := range ev.RawContacts {
		// If this method's governing scope is revoked, drop the raw value.
		governedRevoked := false
		for _, gov := range []string{"CONTACT_CALLBACK"} {
			for _, m := range contactMethodsGovernedBy(gov) {
				if m == method {
					if r, err := scopeRevoked(tx, requestID, gov); err != nil {
						return false, err
					} else if r {
						governedRevoked = true
					}
				}
			}
		}
		if governedRevoked {
			suppressed = true
			continue
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO contact_method(request_id, method, value) VALUES(?,?,?)`,
			requestID, method, value); err != nil {
			return false, fmt.Errorf("store contact method: %w", err)
		}
	}
	return suppressed, nil
}

// GetCanonical loads the fully projected canonical request for a key. The key
// may be the request's own canonical key or any correlation alias attached to
// it, so a caller who only knows one channel's correlation number still reaches
// the single converged request.
func (s *Store) GetCanonical(canonicalKey string) (model.CanonicalRequest, bool, error) {
	var req model.CanonicalRequest
	var id int64
	err := s.db.QueryRow(`SELECT cr.id, cr.canonical_key, cr.version, cr.created_seq, cr.updated_seq
		FROM canonical_request cr
		WHERE cr.canonical_key = ?
		   OR cr.id = (SELECT request_id FROM request_alias WHERE alias_key = ?)`, canonicalKey, canonicalKey).
		Scan(&id, &req.CanonicalKey, &req.Version, &req.CreatedSeq, &req.UpdatedSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return req, false, nil
	}
	if err != nil {
		return req, false, fmt.Errorf("load canonical: %w", err)
	}
	req.ID = id

	// Chain.
	rows, err := s.db.Query(`SELECT seq, event_id, channel, kind, COALESCE(source_id_field,''), COALESCE(source_id,'') FROM event_link WHERE request_id = ? ORDER BY seq`, id)
	if err != nil {
		return req, false, fmt.Errorf("load chain: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var l model.ChainLink
		var kind string
		if err := rows.Scan(&l.Seq, &l.EventID, &l.Channel, &kind, &l.SourceIDField, &l.SourceID); err != nil {
			return req, false, err
		}
		l.Kind = model.EventKind(kind)
		req.Chain = append(req.Chain, l)
	}

	// Accommodations.
	req.Accommodations, err = s.stringList(`SELECT code FROM accommodation WHERE request_id = ? ORDER BY code`, id)
	if err != nil {
		return req, false, err
	}

	// Consent projection.
	crows, err := s.db.Query(`SELECT scope, state FROM consent WHERE request_id = ? ORDER BY scope`, id)
	if err != nil {
		return req, false, fmt.Errorf("load consent: %w", err)
	}
	defer crows.Close()
	contactEffective := false
	for crows.Next() {
		var scope, state string
		if err := crows.Scan(&scope, &state); err != nil {
			return req, false, err
		}
		if state == "GRANTED" {
			req.EffectiveConsent = append(req.EffectiveConsent, scope)
			if scope == "CONTACT_CALLBACK" {
				contactEffective = true
			}
		} else {
			req.RevokedConsent = append(req.RevokedConsent, scope)
		}
	}
	sort.Strings(req.EffectiveConsent)
	sort.Strings(req.RevokedConsent)

	// Contact projection. The callback disposition is non-identifying and always
	// shown; the raw callback window and contact methods (phone/email) are
	// emitted ONLY while CONTACT_CALLBACK is effective. Before consent — or after
	// a sticky revocation — only HasPendingContact signals that details exist.
	req.Contact = model.ContactProjection{Exposed: contactEffective}
	var win sql.NullInt64
	var disp sql.NullString
	_ = s.db.QueryRow(`SELECT callback_window_minutes, callback_disposition FROM contact_detail WHERE request_id = ?`, id).Scan(&win, &disp)
	if disp.Valid {
		req.Contact.CallbackDisposition = disp.String
	}
	methods, err := s.stringList(`SELECT method FROM contact_method WHERE request_id = ? ORDER BY method`, id)
	if err != nil {
		return req, false, err
	}
	hasRaw := win.Valid || len(methods) > 0
	if contactEffective {
		if win.Valid {
			v := int(win.Int64)
			req.Contact.CallbackWindowMinutes = &v
		}
		req.Contact.Methods = methods
	} else {
		// Consent not confirmed (or revoked): withhold raw contact info.
		req.Contact.HasPendingContact = hasRaw
	}
	return req, true, nil
}

func (s *Store) stringList(query string, args ...interface{}) ([]string, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Attempts returns the append-only attempt log for an event, oldest first.
func (s *Store) Attempts(eventID string) ([]model.Attempt, error) {
	rows, err := s.db.Query(`SELECT event_id, attempt_no, COALESCE(canonical_key,''), status, payload_hash, COALESCE(errors_json,''), at FROM attempt WHERE event_id = ? ORDER BY attempt_no`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load attempts: %w", err)
	}
	defer rows.Close()
	return scanAttempts(rows)
}

// AttemptsForKey returns attempts for every event in a canonical request's chain.
func (s *Store) AttemptsForKey(canonicalKey string) ([]model.Attempt, error) {
	rows, err := s.db.Query(`SELECT a.event_id, a.attempt_no, COALESCE(a.canonical_key,''), a.status, a.payload_hash, COALESCE(a.errors_json,''), a.at
		FROM attempt a WHERE a.canonical_key = ? ORDER BY a.id`, canonicalKey)
	if err != nil {
		return nil, fmt.Errorf("load attempts for key: %w", err)
	}
	defer rows.Close()
	return scanAttempts(rows)
}

func scanAttempts(rows *sql.Rows) ([]model.Attempt, error) {
	var out []model.Attempt
	for rows.Next() {
		var a model.Attempt
		var errsJSON string
		if err := rows.Scan(&a.EventID, &a.AttemptNo, &a.CanonicalKey, (*string)(&a.Status), &a.PayloadHash, &errsJSON, &a.At); err != nil {
			return nil, err
		}
		if errsJSON != "" {
			_ = jsonUnmarshal(errsJSON, &a.Errors)
		}
		out = append(out, a)
	}
	return out, nil
}

// AuditTrail returns the append-only audit entries for a canonical key.
func (s *Store) AuditTrail(canonicalKey string) ([]model.AuditEntry, error) {
	rows, err := s.db.Query(`SELECT seq, canonical_key, event_id, action, COALESCE(detail,''), at FROM audit_entry WHERE canonical_key = ? ORDER BY seq`, canonicalKey)
	if err != nil {
		return nil, fmt.Errorf("load audit: %w", err)
	}
	defer rows.Close()
	var out []model.AuditEntry
	for rows.Next() {
		var e model.AuditEntry
		if err := rows.Scan(&e.Seq, &e.CanonicalKey, &e.EventID, &e.Action, &e.Detail, &e.At); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
