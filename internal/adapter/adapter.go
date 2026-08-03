package adapter

import (
	"errors"
	"sync"

	"github.com/accessible-intake/gateway/internal/intake"
)

var ErrTransient = errors.New("downstream legal-services adapter temporarily unavailable")

type Recorder struct {
	mu        sync.Mutex
	delivered map[string][]*intake.CanonicalView
	failNext  int
	failAll   bool
}

func NewRecorder() *Recorder {
	return &Recorder{delivered: map[string][]*intake.CanonicalView{}}
}

func (r *Recorder) FailOnce() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failNext++
}

func (r *Recorder) SetFailAll(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failAll = v
}

func (r *Recorder) Deliver(requestID string, view *intake.CanonicalView) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAll {
		return ErrTransient
	}
	if r.failNext > 0 {
		r.failNext--
		return ErrTransient
	}
	cp := *view
	r.delivered[requestID] = append(r.delivered[requestID], &cp)
	return nil
}

func (r *Recorder) Deliveries(requestID string) []*intake.CanonicalView {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*intake.CanonicalView, len(r.delivered[requestID]))
	copy(out, r.delivered[requestID])
	return out
}
