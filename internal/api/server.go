package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/intake"
	"github.com/accessible-intake/gateway/internal/store"
)

type Server struct {
	Store     *store.Store
	Contracts *contracts.Contracts
	Adapter   store.Adapter
}

type submitResponse struct {
	Result      *intake.EventResult   `json:"result"`
	Canonical   *intake.CanonicalView `json:"canonical,omitempty"`
	Conflict    bool                  `json:"conflict,omitempty"`
	ConflictMsg string                `json:"conflictMessage,omitempty"`
}

type errorResponse struct {
	Error       string              `json:"error"`
	ItemResults []intake.ItemResult `json:"itemResults,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func NewServer(s *store.Store, c *contracts.Contracts, adapter store.Adapter) *Server {
	return &Server{Store: s, Contracts: c, Adapter: adapter}
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/events", s.HandleSubmit)
	mux.HandleFunc("/api/v1/events/", s.HandleEvent)
	mux.HandleFunc("/api/v1/requests/", s.HandleRequest)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (s *Server) HandleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	var raw json.RawMessage
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	env, hard, err := intake.DecodeEnvelope(raw, s.Contracts)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error(), ItemResults: hard})
		return
	}

	items := intake.Validate(env, s.Contracts)
	hash := intake.PayloadHash(env)

	hasRejected := false
	for _, it := range items {
		if it.Status == intake.ItemRejected {
			hasRejected = true
		}
	}
	status := intake.StatusAccepted
	if hasRejected {
		status = intake.StatusAcceptedWithErrors
	}

	outcome, err := s.Store.Submit(&store.SubmitInput{
		Envelope:    env,
		ItemResults: items,
		PayloadHash: hash,
		RawEnvelope: raw,
		Adapter:     s.Adapter,
		Status:      status,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if outcome.Conflict {
		writeJSON(w, http.StatusConflict, submitResponse{
			Result: outcome.Result, Canonical: outcome.Projection,
			Conflict: true, ConflictMsg: outcome.ConflictDetail,
		})
		return
	}
	code := http.StatusOK
	if outcome.Result.Status == intake.StatusAdapterFailure {
		code = http.StatusAccepted
	}
	writeJSON(w, code, submitResponse{Result: outcome.Result, Canonical: outcome.Projection})
}

func (s *Server) HandleEvent(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/events/")
	parts := strings.Split(path, "/")
	if parts[0] == "" {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
		return
	}
	eventID := parts[0]
	if len(parts) == 2 && parts[1] == "retry" {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
			return
		}
		res, view, err := s.Store.RetryDelivery(eventID, s.Adapter)
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "event not found"})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		code := http.StatusOK
		if res.DeliveryStatus == intake.DeliveryPending {
			code = http.StatusAccepted
		}
		writeJSON(w, code, submitResponse{Result: res, Canonical: view})
		return
	}
	if len(parts) != 1 {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}
	res, err := s.Store.EventByID(eventID)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "event not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) HandleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/requests/")
	parts := strings.Split(path, "/")
	if parts[0] == "" || len(parts) > 2 {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
		return
	}
	requestID := parts[0]
	if len(parts) == 2 {
		switch parts[1] {
		case "events":
			chain, err := s.Store.EventChain(requestID)
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusNotFound, errorResponse{Error: "request not found"})
				return
			}
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"requestId": requestID, "events": chain})
			return
		case "audit":
			summary, err := s.Store.AuditSummary(requestID)
			if errors.Is(err, store.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, errorResponse{Error: "request not found"})
				return
			}
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, summary)
			return
		default:
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
			return
		}
	}
	view, err := s.Store.Project(requestID)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "request not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, view)
}
