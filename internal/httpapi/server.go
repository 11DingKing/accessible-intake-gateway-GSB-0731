// Package httpapi exposes the unified intake HTTP API using only the Go
// standard library (net/http).
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/accessible-intake/gateway/internal/canonical"
	"github.com/accessible-intake/gateway/internal/intake"
	"github.com/accessible-intake/gateway/internal/store"
)

// Server wires the intake service to HTTP routes.
type Server struct {
	svc *intake.Service
	mux *http.ServeMux
}

// New constructs an HTTP server with all routes registered.
func New(svc *intake.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /v1/intake/events", s.submitEvent)
	s.mux.HandleFunc("GET /v1/intake/events/{eventId}", s.getEvent)
	s.mux.HandleFunc("GET /v1/intake/events/{eventId}/attempts", s.getAttempts)
	s.mux.HandleFunc("GET /v1/intake/canonical/{id}", s.getCanonical)
	s.mux.HandleFunc("GET /v1/intake/canonical/{id}/audit", s.getAudit)
	s.mux.HandleFunc("POST /v1/intake/canonical/{id}/replay", s.replay)
}

// Handler returns the http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) submitEvent(w http.ResponseWriter, r *http.Request) {
	var env canonical.EventEnvelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON: "+err.Error())
		return
	}
	result, err := s.svc.Submit(r.Context(), env)
	if err != nil {
		var se *intake.SubmitError
		if errors.As(err, &se) {
			if se.Result != nil {
				writeJSON(w, se.Status, se.Result)
				return
			}
			writeError(w, se.Status, se.Code, se.Message)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("eventId")
	row, results, err := s.svc.Event(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "event not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"eventId":          row.EventID,
		"canonicalRequestId": row.CanonicalID,
		"channel":          row.Channel,
		"sourceId":         row.SourceID,
		"sourceIdField":    row.SourceIDField,
		"payloadHash":      row.PayloadHash,
		"sequence":         row.Sequence,
		"status":           row.Status,
		"itemResults":      results,
		"createdAt":        row.CreatedAt,
	})
}

func (s *Server) getAttempts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("eventId")
	atts, err := s.svc.Attempts(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	if atts == nil {
		atts = []canonical.Attempt{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"eventId": id, "attempts": atts})
}

func (s *Server) getCanonical(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	snap, err := s.svc.Canonical(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "canonical request not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) getAudit(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	summary, err := s.svc.Audit(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "canonical request not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	snap, events, err := s.svc.Replay(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "canonical request not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"canonicalRequestId": id,
		"reconstructed":      snap,
		"eventsApplied":      events,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{
		"error":   code,
		"message": msg,
	})
}

// contentTypeIsJSON is a small helper for callers that need to gate behavior.
func contentTypeIsJSON(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "application/json")
}
