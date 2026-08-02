// Package contract loads and exposes the channel contract fixture
// (materials/channel-contracts.json). The fixture is the single source of
// truth for channel IDs, consent scopes, accommodation codes, and the
// minute-based callback field; nothing here silently renames them.
package contract

import (
	"encoding/json"
	"fmt"
	"os"
)

// Channel describes how a raw envelope for a given channel is shaped.
type Channel struct {
	SourceIDField       string `json:"sourceIdField"`
	PersonField         string `json:"personField"`
	CallbackWindowField string `json:"callbackWindowField,omitempty"`
}

// Contract is the parsed channel-contracts.json fixture.
type Contract struct {
	CanonicalVersion  string             `json:"canonicalVersion"`
	AccommodationCodes []string          `json:"accommodationCodes"`
	ConsentScopes     []string           `json:"consentScopes"`
	Channels          map[string]Channel `json:"channels"`

	accommodationSet map[string]struct{}
	consentSet       map[string]struct{}
}

// Load reads and validates the contract fixture from path.
func Load(path string) (*Contract, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read contract: %w", err)
	}
	return Parse(raw)
}

// Parse validates and indexes a raw contract document.
func Parse(raw []byte) (*Contract, error) {
	var c Contract
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse contract: %w", err)
	}
	if c.CanonicalVersion == "" {
		return nil, fmt.Errorf("contract: canonicalVersion is required")
	}
	if len(c.Channels) == 0 {
		return nil, fmt.Errorf("contract: at least one channel is required")
	}
	c.accommodationSet = toSet(c.AccommodationCodes)
	c.consentSet = toSet(c.ConsentScopes)
	for id, ch := range c.Channels {
		if ch.SourceIDField == "" {
			return nil, fmt.Errorf("contract: channel %q missing sourceIdField", id)
		}
		if ch.PersonField == "" {
			return nil, fmt.Errorf("contract: channel %q missing personField", id)
		}
	}
	return &c, nil
}

// Channel returns the channel definition and whether it is known.
func (c *Contract) Channel(id string) (Channel, bool) {
	ch, ok := c.Channels[id]
	return ch, ok
}

// KnownAccommodation reports whether code is a recognized accommodation code.
func (c *Contract) KnownAccommodation(code string) bool {
	_, ok := c.accommodationSet[code]
	return ok
}

// KnownConsentScope reports whether scope is a recognized consent scope.
func (c *Contract) KnownConsentScope(scope string) bool {
	_, ok := c.consentSet[scope]
	return ok
}

func toSet(items []string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[it] = struct{}{}
	}
	return m
}
