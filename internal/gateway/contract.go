package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// ChannelContract describes how one delivery channel names its fields.
// Field names come verbatim from materials/channel-contracts.json and are
// never renamed.
type ChannelContract struct {
	SourceIDField       string `json:"sourceIdField"`
	PersonField         string `json:"personField"`
	CallbackWindowField string `json:"callbackWindowField,omitempty"`
}

// Registry is the loaded channel contract: the single source of truth for
// channel IDs, accommodation codes, consent scopes and the canonical version.
type Registry struct {
	CanonicalVersion   string                     `json:"canonicalVersion"`
	AccommodationCodes []string                   `json:"accommodationCodes"`
	ConsentScopes      []string                   `json:"consentScopes"`
	Channels           map[string]ChannelContract `json:"channels"`

	accommodationSet map[string]bool
	consentSet       map[string]bool
}

// LoadRegistry reads and validates the channel contract file.
func LoadRegistry(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read channel contract: %w", err)
	}
	var reg Registry
	if err := json.Unmarshal(data, &reg); err != nil {
		return nil, fmt.Errorf("parse channel contract: %w", err)
	}
	if reg.CanonicalVersion == "" {
		return nil, fmt.Errorf("channel contract: canonicalVersion is required")
	}
	if len(reg.Channels) == 0 {
		return nil, fmt.Errorf("channel contract: at least one channel is required")
	}
	for id, ch := range reg.Channels {
		if ch.SourceIDField == "" || ch.PersonField == "" {
			return nil, fmt.Errorf("channel contract: channel %q requires sourceIdField and personField", id)
		}
	}
	reg.accommodationSet = make(map[string]bool, len(reg.AccommodationCodes))
	for _, code := range reg.AccommodationCodes {
		reg.accommodationSet[code] = true
	}
	reg.consentSet = make(map[string]bool, len(reg.ConsentScopes))
	for _, scope := range reg.ConsentScopes {
		reg.consentSet[scope] = true
	}
	return &reg, nil
}

// KnownAccommodation reports whether code is a registered accommodation code.
func (r *Registry) KnownAccommodation(code string) bool { return r.accommodationSet[code] }

// KnownConsentScope reports whether scope is a registered consent scope.
func (r *Registry) KnownConsentScope(scope string) bool { return r.consentSet[scope] }

// Channel returns the contract for a channel ID.
func (r *Registry) Channel(id string) (ChannelContract, bool) {
	ch, ok := r.Channels[id]
	return ch, ok
}

// ChannelIDs returns sorted channel IDs for deterministic output.
func (r *Registry) ChannelIDs() []string {
	ids := make([]string, 0, len(r.Channels))
	for id := range r.Channels {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
