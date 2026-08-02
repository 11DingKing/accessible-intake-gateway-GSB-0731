package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/accessible-intake-gateway/internal/identity"
	"github.com/accessible-intake-gateway/internal/model"
)

// resolve deterministically binds an event to exactly one canonical request,
// creating a new request only when nothing matches. It runs inside the caller's
// serialized transaction, so out-of-order arrival, concurrent claims from two
// channels, and adapter-failure retries all converge to a single chain: the
// first writer to claim a correlation number or identity fragment owns it, and
// every later event carrying that signal resolves to the same request.
//
// Matching precedence:
//  1. The source correlation number is a decisive unique key. If it already
//     maps to a request (directly or as an alias), that request wins.
//  2. Otherwise corroborating fragments (phone/email/name/dob/postal) are used.
//     They may attach a brand-new correlation number to an existing request
//     (the callback-before-web-build case), but never merge two requests that
//     carry different unique correlation numbers.
//  3. Otherwise a new request is created.
func resolve(tx *sql.Tx, ev *model.CanonicalEvent) (int64, string, bool, model.MatchDecision, error) {
	var dec model.MatchDecision

	var correlation *model.IdentityFragment
	var corr []model.IdentityFragment
	for i := range ev.Fragments {
		f := ev.Fragments[i]
		if f.Type == identity.FragCorrelation {
			c := f
			correlation = &c
		} else {
			corr = append(corr, f)
		}
	}

	// Classify corroborating fragments against whichever request they already
	// belong to. matchedHere: fragments owned by the bound request; elsewhere:
	// fragments owned by some *other* request (evidence of a partial conflict).
	classify := func(reqID int64) (matched, elsewhere []string) {
		for _, f := range corr {
			owner, ok, err := fragmentOwner(tx, f.Hash)
			if err != nil {
				continue
			}
			if ok && owner == reqID {
				matched = append(matched, f.Type)
			} else if ok && owner != reqID {
				elsewhere = append(elsewhere, f.Type)
			}
		}
		return
	}

	// 1. Decisive correlation match.
	if correlation != nil {
		if reqID, ok, err := lookupByAliasOrFragment(tx, ev.CanonicalKey, correlation.Hash); err != nil {
			return 0, "", false, dec, err
		} else if ok {
			key, err := canonicalKeyOf(tx, reqID)
			if err != nil {
				return 0, "", false, dec, err
			}
			matched, elsewhere := classify(reqID)
			if err := attachFragments(tx, reqID, ev.Fragments); err != nil {
				return 0, "", false, dec, err
			}
			dec = decision(key, "CORRELATION_MATCH", append([]string{identity.FragCorrelation}, matched...), elsewhere, false)
			return reqID, key, false, dec, nil
		}

		// Correlation is new. It may still corroborate to an existing request,
		// but only if that request does not already carry a *different*
		// correlation number (different unique => different request).
		if reqID, ok, err := singleCorroboratingRequest(tx, corr); err != nil {
			return 0, "", false, dec, err
		} else if ok {
			conflicting, err := requestHasConflictingUnique(tx, reqID, correlation.Hash)
			if err != nil {
				return 0, "", false, dec, err
			}
			if !conflicting {
				key, err := canonicalKeyOf(tx, reqID)
				if err != nil {
					return 0, "", false, dec, err
				}
				if err := attachAlias(tx, ev.CanonicalKey, reqID); err != nil {
					return 0, "", false, dec, err
				}
				if err := attachFragments(tx, reqID, ev.Fragments); err != nil {
					return 0, "", false, dec, err
				}
				matched, elsewhere := classify(reqID)
				dec = decision(key, "CORROBORATED_MATCH", matched, elsewhere, false)
				return reqID, key, false, dec, nil
			}
			// Different unique correlation: keep them apart, record the shared
			// corroborating fragments as conflict evidence.
			reqID, key, err := createRequest(tx, ev.CanonicalKey, ev.Fragments)
			if err != nil {
				return 0, "", false, dec, err
			}
			_, elsewhere := classify(reqID)
			dec = decision(key, "UNIQUE_CONFLICT_NEW_REQUEST", []string{identity.FragCorrelation}, elsewhere, true)
			return reqID, key, true, dec, nil
		}

		// Brand-new correlation with no corroboration: fresh request.
		reqID, key, err := createRequest(tx, ev.CanonicalKey, ev.Fragments)
		if err != nil {
			return 0, "", false, dec, err
		}
		dec = decision(key, "NEW_CORRELATION", []string{identity.FragCorrelation}, nil, true)
		return reqID, key, true, dec, nil
	}

	// 2. No correlation number (e.g. a hotline callback before the web build):
	// resolve purely from corroborating fragments.
	if len(corr) > 0 {
		rids, err := corroboratingRequests(tx, corr)
		if err != nil {
			return 0, "", false, dec, err
		}
		switch len(rids) {
		case 1:
			reqID := rids[0]
			key, err := canonicalKeyOf(tx, reqID)
			if err != nil {
				return 0, "", false, dec, err
			}
			matched, elsewhere := classify(reqID)
			if err := attachFragments(tx, reqID, ev.Fragments); err != nil {
				return 0, "", false, dec, err
			}
			dec = decision(key, "CORROBORATED_MATCH", matched, elsewhere, false)
			return reqID, key, false, dec, nil
		case 0:
			// New request keyed by a deterministic synthetic correlation derived
			// from the fragment set (stable across replay).
			synthetic := syntheticKey(ev.Fragments)
			reqID, key, err := createRequest(tx, synthetic, ev.Fragments)
			if err != nil {
				return 0, "", false, dec, err
			}
			var matched []string
			for _, f := range corr {
				matched = append(matched, f.Type)
			}
			dec = decision(key, "NEW_FRAGMENTS", matched, nil, true)
			return reqID, key, true, dec, nil
		default:
			// Ambiguous: fragments point at multiple requests. Bind
			// deterministically to the earliest (lowest id) and record the rest
			// as conflict evidence — never spawn a second chain.
			sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
			reqID := rids[0]
			key, err := canonicalKeyOf(tx, reqID)
			if err != nil {
				return 0, "", false, dec, err
			}
			matched, elsewhere := classify(reqID)
			if err := attachFragments(tx, reqID, ev.Fragments); err != nil {
				return 0, "", false, dec, err
			}
			dec = decision(key, "AMBIGUOUS_MATCH", matched, elsewhere, false)
			return reqID, key, false, dec, nil
		}
	}

	// 3. No identity signals at all: fall back to channel + source id (Round 1
	// behavior for single-event requests).
	fallback := ev.Channel + ":" + ev.SourceID
	reqID, key, created, err := createOrGetByKey(tx, fallback)
	if err != nil {
		return 0, "", false, dec, err
	}
	dec = decision(key, "CHANNEL_SOURCE_FALLBACK", nil, nil, created)
	return reqID, key, created, dec, nil
}

func decision(key, reason string, matched, conflicting []string, newReq bool) model.MatchDecision {
	conf, score := identity.Score(matched)
	d := model.MatchDecision{
		CanonicalKey:  key,
		Reason:        reason,
		Confidence:    conf,
		Score:         score,
		MatchedOn:     identity.SortTypes(dedupe(matched)),
		ConflictingOn: identity.SortTypes(dedupe(conflicting)),
		NewRequest:    newReq,
	}
	return d
}

func dedupe(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// fragmentOwner returns the request id that owns a fragment hash.
func fragmentOwner(tx *sql.Tx, fragHash string) (int64, bool, error) {
	var rid int64
	err := tx.QueryRow(`SELECT request_id FROM identity_fragment WHERE frag_hash = ?`, fragHash).Scan(&rid)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("fragment owner: %w", err)
	}
	return rid, true, nil
}

// lookupByAliasOrFragment resolves a correlation number to a request via the
// alias index first (its canonical/alias key), then the fragment index.
func lookupByAliasOrFragment(tx *sql.Tx, aliasKey, correlationHash string) (int64, bool, error) {
	if aliasKey != "" {
		var rid int64
		err := tx.QueryRow(`SELECT request_id FROM request_alias WHERE alias_key = ?`, aliasKey).Scan(&rid)
		if err == nil {
			return rid, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, false, fmt.Errorf("alias lookup: %w", err)
		}
	}
	return fragmentOwner(tx, correlationHash)
}

// corroboratingRequests returns the distinct request ids owning any of the
// given corroborating fragments.
func corroboratingRequests(tx *sql.Tx, corr []model.IdentityFragment) ([]int64, error) {
	seen := map[int64]struct{}{}
	var out []int64
	for _, f := range corr {
		rid, ok, err := fragmentOwner(tx, f.Hash)
		if err != nil {
			return nil, err
		}
		if ok {
			if _, dup := seen[rid]; !dup {
				seen[rid] = struct{}{}
				out = append(out, rid)
			}
		}
	}
	return out, nil
}

func singleCorroboratingRequest(tx *sql.Tx, corr []model.IdentityFragment) (int64, bool, error) {
	rids, err := corroboratingRequests(tx, corr)
	if err != nil {
		return 0, false, err
	}
	if len(rids) == 1 {
		return rids[0], true, nil
	}
	return 0, false, nil
}

// requestHasConflictingUnique reports whether a request already owns a unique
// fragment (correlation) different from the one offered.
func requestHasConflictingUnique(tx *sql.Tx, reqID int64, offeredUniqueHash string) (bool, error) {
	rows, err := tx.Query(`SELECT frag_hash FROM identity_fragment WHERE request_id = ? AND is_unique = 1`, reqID)
	if err != nil {
		return false, fmt.Errorf("unique scan: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return false, err
		}
		if h != offeredUniqueHash {
			return true, nil
		}
	}
	return false, nil
}

func canonicalKeyOf(tx *sql.Tx, reqID int64) (string, error) {
	var key string
	err := tx.QueryRow(`SELECT canonical_key FROM canonical_request WHERE id = ?`, reqID).Scan(&key)
	if err != nil {
		return "", fmt.Errorf("canonical key of %d: %w", reqID, err)
	}
	return key, nil
}

// createRequest creates a new canonical request with the given key, registers
// the key as an alias, and attaches all of the event's fragments.
func createRequest(tx *sql.Tx, key string, frags []model.IdentityFragment) (int64, string, error) {
	reqID, resolvedKey, _, err := createOrGetByKey(tx, key)
	if err != nil {
		return 0, "", err
	}
	if err := attachFragments(tx, reqID, frags); err != nil {
		return 0, "", err
	}
	return reqID, resolvedKey, nil
}

// createOrGetByKey inserts a canonical request for key if absent (safe under
// the serialized connection) and always registers the alias. It returns the
// request id, its canonical key, and whether the request was newly created.
func createOrGetByKey(tx *sql.Tx, key string) (int64, string, bool, error) {
	seq, _ := currentSeq(tx)
	r, err := tx.Exec(`INSERT OR IGNORE INTO canonical_request(canonical_key, version, created_seq, updated_seq) VALUES(?, 'intake.v1', ?, ?)`,
		key, seq+1, seq+1)
	if err != nil {
		return 0, "", false, fmt.Errorf("create request: %w", err)
	}
	created := false
	if n, _ := r.RowsAffected(); n > 0 {
		created = true
	}
	var rid int64
	if err := tx.QueryRow(`SELECT id FROM canonical_request WHERE canonical_key = ?`, key).Scan(&rid); err != nil {
		return 0, "", false, fmt.Errorf("load request id: %w", err)
	}
	if err := attachAlias(tx, key, rid); err != nil {
		return 0, "", false, err
	}
	return rid, key, created, nil
}

func attachAlias(tx *sql.Tx, aliasKey string, reqID int64) error {
	if aliasKey == "" {
		return nil
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO request_alias(alias_key, request_id) VALUES(?,?)`, aliasKey, reqID); err != nil {
		return fmt.Errorf("attach alias: %w", err)
	}
	return nil
}

// attachFragments records each fragment's ownership. INSERT OR IGNORE means the
// first request to claim a fragment keeps it, so replays and concurrent claims
// do not steal fragments or fork the chain.
func attachFragments(tx *sql.Tx, reqID int64, frags []model.IdentityFragment) error {
	for _, f := range frags {
		unique := 0
		if f.Unique {
			unique = 1
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO identity_fragment(frag_hash, request_id, frag_type, is_unique) VALUES(?,?,?,?)`,
			f.Hash, reqID, f.Type, unique); err != nil {
			return fmt.Errorf("attach fragment: %w", err)
		}
	}
	return nil
}

// syntheticKey derives a deterministic canonical key from a fragment set, used
// when a request is created before any correlation number is known.
func syntheticKey(frags []model.IdentityFragment) string {
	hashes := make([]string, 0, len(frags))
	for _, f := range frags {
		hashes = append(hashes, f.Hash)
	}
	sort.Strings(hashes)
	return "frag:" + identity.Hash("synthetic", strings.Join(hashes, "|"))[:24]
}
