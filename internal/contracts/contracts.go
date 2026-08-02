// Package contracts loads and exposes the canonical channel fixture from
// materials/channel-contracts.json. Field names, channel IDs, consent scopes,
// accommodation codes, and the minute-based callback field are kept exactly as
// declared in the fixture.
package contracts

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
)

//go:embed channel-contracts.json
var raw []byte

// Channel describes one intake channel and its native field names.
type Channel struct {
	SourceIDField      string `json:"sourceIdField"`
	PersonField        string `json:"personField"`
	CallbackWindowField string `json:"callbackWindowField,omitempty"`
}

// Contracts is the parsed fixture.
type Contracts struct {
	CanonicalVersion    string              `json:"canonicalVersion"`
	AccommodationCodes  []string            `json:"accommodationCodes"`
	ConsentScopes       []string            `json:"consentScopes"`
	Channels            map[string]Channel  `json:"channels"`
	Examples            []map[string]any    `json:"examples"`
	Revocation          map[string]any      `json:"revocation"`
	InvalidCases        []string            `json:"invalidCases"`

	accommodationSet map[string]struct{}
	consentSet       map[string]struct{}
}

var (
	loaded *Contracts
	once   sync.Once
	err    error
)

// Load returns the singleton parsed fixture.
func Load() (*Contracts, error) {
	once.Do(func() {
		var c Contracts
		if e := json.Unmarshal(raw, &c); e != nil {
			err = fmt.Errorf("parse channel-contracts.json: %w", e)
			return
		}
		c.accommodationSet = toSet(c.AccommodationCodes)
		c.consentSet = toSet(c.ConsentScopes)
		loaded = &c
	})
	return loaded, err
}

// MustLoad panics if the fixture cannot be parsed; used at startup.
func MustLoad() *Contracts {
	c, e := Load()
	if e != nil {
		panic(e)
	}
	return c
}

func toSet(items []string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[it] = struct{}{}
	}
	return m
}

// IsChannel reports whether id is a declared channel.
func (c *Contracts) IsChannel(id string) bool {
	_, ok := c.Channels[id]
	return ok
}

// IsAccommodation reports whether code is a declared accommodation code.
func (c *Contracts) IsAccommodation(code string) bool {
	_, ok := c.accommodationSet[code]
	return ok
}

// IsConsentScope reports whether scope is a declared consent scope.
func (c *Contracts) IsConsentScope(scope string) bool {
	_, ok := c.consentSet[scope]
	return ok
}

// CanonicalVersion returns the declared canonical schema version.
func (c *Contracts) Version() string { return c.CanonicalVersion }
