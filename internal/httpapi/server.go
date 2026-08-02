// Package httpapi exposes the intake service over a standard-library HTTP mux.
// No third-party web framework is used.
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/accessible-intake-gateway/internal/intake"
	"github.com/accessible-intake-gateway/internal/model"
)

// Server holds the intake service and its HTTP routes.
type Server struct {
	svc *intake.Service
	mux *http.ServeMux
}

// New builds a Server with all routes registered.
func New(svc *intake.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the underlying http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /v1/intake/events", s.handleSubmit)
	s.mux.HandleFunc("GET /v1/requests/{key}", s.handleGetRequest)
	s.mux.HandleFunc("GET /v1/requests/{key}/summary", s.handleGetSummary)
	s.mux.HandleFunc("GET /v1/events/{id}/attempts", s.handleGetAttempts)
}

type submitRequest struct {
	Events []json.RawMessage `json:"events"`
}

type submitResponse struct {
	Results []model.RecordResult `json:"results"`
	Summary batchSummary         `json:"summary"`
}

type batchSummary struct {
	Total     int `json:"total"`
	Accepted  int `json:"accepted"`
	Duplicate int `json:"duplicate"`
	Conflict  int `json:"conflict"`
	Rejected  int `json:"rejected"`
	Failed    int `json:"failed"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req submitRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		// Allow a single bare envelope as a convenience.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {\"events\":[...]}: " + err.Error()})
		return
	}
	if len(req.Events) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "events must be a non-empty array"})
		return
	}
	results, _ := s.svc.SubmitBatch(r.Context(), req.Events)

	resp := submitResponse{Results: results}
	for _, res := range results {
		resp.Summary.Total++
		switch {
		case res.Duplicate:
			resp.Summary.Duplicate++
		case res.Status == model.StatusAccepted:
			resp.Summary.Accepted++
		case res.Status == model.StatusConflict:
			resp.Summary.Conflict++
		case res.Status == model.StatusRejected:
			resp.Summary.Rejected++
		case res.Status == model.StatusFailed:
			resp.Summary.Failed++
		}
	}
	// 207-style semantics collapsed onto 200: per-record status lives in results.
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	req, ok, err := s.svc.Store().GetCanonical(key)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "canonical request not found: " + key})
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func (s *Server) handleGetSummary(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	sum, ok, err := s.svc.Store().Summary(key)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "canonical request not found: " + key})
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleGetAttempts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	attempts, err := s.svc.Store().Attempts(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"eventId": id, "attempts": attempts})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":           "ok",
		"canonicalVersion": s.svc.Contract().CanonicalVersion,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
