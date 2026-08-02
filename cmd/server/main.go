// Command server runs the Accessible Intake Gateway HTTP service.
//
// Usage:
//
//	go run ./cmd/server                 # serves on :8080, DB at ./intake.db
//	INTAKE_ADDR=:9090 go run ./cmd/server
//	INTAKE_DB=/tmp/x.db  go run ./cmd/server
//	INTAKE_CONTRACT=materials/channel-contracts.json go run ./cmd/server
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/accessible-intake-gateway/internal/contract"
	"github.com/accessible-intake-gateway/internal/httpapi"
	"github.com/accessible-intake-gateway/internal/intake"
	"github.com/accessible-intake-gateway/internal/store"
)

func main() {
	addr := getenv("INTAKE_ADDR", ":8080")
	dbPath := getenv("INTAKE_DB", "intake.db")
	contractPath := getenv("INTAKE_CONTRACT", "materials/channel-contracts.json")

	c, err := contract.Load(contractPath)
	if err != nil {
		log.Fatalf("load contract: %v", err)
	}
	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	svc := intake.New(c, st, nil)
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(svc).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	idle := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
		close(idle)
	}()

	log.Printf("accessible-intake-gateway listening on %s (db=%s, contract=%s, version=%s)",
		srv.Addr, dbPath, contractPath, c.CanonicalVersion)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
	<-idle
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
