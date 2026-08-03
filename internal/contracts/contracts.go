package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type ChannelSpec struct {
	SourceIDField       string `json:"sourceIdField"`
	PersonField         string `json:"personField"`
	CallbackWindowField string `json:"callbackWindowField,omitempty"`
}

type Contracts struct {
	CanonicalVersion   string                 `json:"canonicalVersion"`
	AccommodationCodes []string               `json:"accommodationCodes"`
	ConsentScopes      []string               `json:"consentScopes"`
	Channels           map[string]ChannelSpec `json:"channels"`
}

var (
	loaded *Contracts
	once   sync.Once
)

func Load(path string) (*Contracts, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read channel contracts: %w", err)
	}
	var c Contracts
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse channel contracts: %w", err)
	}
	if c.CanonicalVersion == "" {
		return nil, fmt.Errorf("channel contracts missing canonicalVersion")
	}
	if len(c.Channels) == 0 {
		return nil, fmt.Errorf("channel contracts missing channels")
	}
	return &c, nil
}

func Default() *Contracts {
	once.Do(func() {
		c, err := Load("materials/channel-contracts.json")
		if err != nil {
			panic(err)
		}
		loaded = c
	})
	return loaded
}

func (c *Contracts) IsChannel(ch string) bool {
	_, ok := c.Channels[ch]
	return ok
}

func (c *Contracts) IsAccommodation(code string) bool {
	for _, v := range c.AccommodationCodes {
		if v == code {
			return true
		}
	}
	return false
}

func (c *Contracts) IsConsentScope(scope string) bool {
	for _, v := range c.ConsentScopes {
		if v == scope {
			return true
		}
	}
	return false
}
