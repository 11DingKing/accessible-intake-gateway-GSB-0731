package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// maxBodyBytes caps any request body; batch record count is capped separately.
const (
	maxBodyBytes    = 1 << 20 // 1 MiB
	maxBatchRecords = 500
)

// Handler exposes the intake API over the standard library HTTP stack.
type Handler struct {
	svc *Service
	reg *Registry
	mux *http.ServeMux
}

// NewHandler wires all routes.
func NewHandler(svc *Service, reg *Registry) *Handler {
	h := &Handler{svc: svc, reg: reg, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/intake/events", h.postEvent)
	h.mux.HandleFunc("POST /v1/intake/batches", h.postBatch)
	h.mux.HandleFunc("GET /v1/requests/{requestId}", h.getRequest)
	h.mux.HandleFunc("GET /v1/requests/{requestId}/events", h.getRequestEvents)
	h.mux.HandleFunc("GET /v1/requests/{requestId}/audit", h.getRequestAudit)
	h.mux.HandleFunc("GET /v1/requests/{requestId}/attempts", h.getRequestAttempts)
	h.mux.HandleFunc("GET /healthz", h.healthz)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// postEvent ingests one channel envelope.
func (h *Handler) postEvent(w http.ResponseWriter, r *http.Request) {
	raw, err := readBody(w, r)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, CodeInvalidEnvelope, err.Error())
		return
	}
	result, status := h.svc.ProcessEnvelope(r.Context(), raw)
	writeJSON(w, status, result)
}

// batchRequest wraps a batch of records; a bare JSON array is also accepted.
type batchRequest struct {
	Records []json.RawMessage `json:"records"`
}

// postBatch ingests a mixed batch. Each record is processed in its own
// transaction, so the response is always 200 with per-record outcomes.
func (h *Handler) postBatch(w http.ResponseWriter, r *http.Request) {
	raw, err := readBody(w, r)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, CodeInvalidEnvelope, err.Error())
		return
	}
	var records []json.RawMessage
	trimmed := trimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &records); err != nil {
			writeError(w, http.StatusBadRequest, CodeInvalidJSON, "batch body must be a JSON array or {\"records\": [...]}: "+err.Error())
			return
		}
	} else {
		var req batchRequest
		if err := json.Unmarshal(trimmed, &req); err != nil {
			writeError(w, http.StatusBadRequest, CodeInvalidJSON, "batch body must be a JSON array or {\"records\": [...]}: "+err.Error())
			return
		}
		records = req.Records
	}
	if len(records) == 0 {
		writeError(w, http.StatusBadRequest, CodeInvalidEnvelope, "batch contains no records")
		return
	}
	if len(records) > maxBatchRecords {
		writeError(w, http.StatusRequestEntityTooLarge, CodeInvalidEnvelope, "batch exceeds the 500 record limit")
		return
	}
	results := make([]RecordResult, 0, len(records))
	for _, record := range records {
		result, _ := h.svc.ProcessEnvelope(r.Context(), record)
		results = append(results, result)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (h *Handler) getRequest(w http.ResponseWriter, r *http.Request) {
	view, err := h.svc.GetRequest(r.Context(), r.PathValue("requestId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, CodeTransientFailure, err.Error())
		return
	}
	if view == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "request not found")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) getRequestEvents(w http.ResponseWriter, r *http.Request) {
	events, found, err := h.svc.ListEvents(r.Context(), r.PathValue("requestId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, CodeTransientFailure, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, CodeNotFound, "request not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (h *Handler) getRequestAudit(w http.ResponseWriter, r *http.Request) {
	audit, err := h.svc.GetAudit(r.Context(), r.PathValue("requestId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, CodeTransientFailure, err.Error())
		return
	}
	if audit == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "request not found")
		return
	}
	writeJSON(w, http.StatusOK, audit)
}

func (h *Handler) getRequestAttempts(w http.ResponseWriter, r *http.Request) {
	attempts, found, err := h.svc.ListAttempts(r.Context(), r.PathValue("requestId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, CodeTransientFailure, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, CodeNotFound, "request not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"attempts": attempts})
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "canonicalVersion": h.reg.CanonicalVersion})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return nil, errors.New("request body unreadable or exceeds the 1 MiB limit")
	}
	if len(trimSpace(body)) == 0 {
		return nil, errors.New("request body is empty")
	}
	return body, nil
}

func trimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\t' || b[start] == '\n' || b[start] == '\r') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\n' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
