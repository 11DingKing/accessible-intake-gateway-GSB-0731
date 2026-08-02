// Package intake wires the contract, normalizer, and store together into the
// unified acceptance service. It processes batches record-by-record: each
// event is normalized and applied under its own transaction, so one bad or
// failing record never rolls back its neighbours. Ordering within a batch does
// not matter — convergence is by canonical key, not arrival order.
package intake

import (
	"context"
	"encoding/json"

	"github.com/accessible-intake-gateway/internal/contract"
	"github.com/accessible-intake-gateway/internal/model"
	"github.com/accessible-intake-gateway/internal/normalize"
	"github.com/accessible-intake-gateway/internal/store"
)

// Service is the unified intake acceptance service.
type Service struct {
	contract *contract.Contract
	store    *store.Store
	down     store.Downstream
}

// New creates a Service. down may be nil when no downstream adapter is wired.
func New(c *contract.Contract, s *store.Store, down store.Downstream) *Service {
	return &Service{contract: c, store: s, down: down}
}

// Contract exposes the loaded contract (used by the HTTP layer for metadata).
func (svc *Service) Contract() *contract.Contract { return svc.contract }

// Store exposes the persistence layer for read endpoints.
func (svc *Service) Store() *store.Store { return svc.store }

// SubmitOne normalizes and applies a single raw envelope.
func (svc *Service) SubmitOne(ctx context.Context, raw json.RawMessage) (model.RecordResult, error) {
	ev, verrs := normalize.Normalize(svc.contract, raw)
	return svc.store.Apply(ctx, ev, verrs, svc.down)
}

// SubmitBatch processes each envelope independently and returns one result per
// input, in input order. A failure on any single record is captured in that
// record's result and does not abort the rest of the batch.
func (svc *Service) SubmitBatch(ctx context.Context, raws []json.RawMessage) ([]model.RecordResult, error) {
	results := make([]model.RecordResult, 0, len(raws))
	for _, raw := range raws {
		res, err := svc.SubmitOne(ctx, raw)
		if err != nil {
			// Infrastructure-level error (e.g. DB failure): surface as a
			// retriable FAILED result rather than dropping the record.
			res = model.RecordResult{
				Status:    model.StatusFailed,
				Retriable: true,
				Errors:    []model.RecordError{{Field: "_", Code: "INTERNAL", Message: err.Error()}},
			}
		}
		results = append(results, res)
	}
	return results, nil
}
