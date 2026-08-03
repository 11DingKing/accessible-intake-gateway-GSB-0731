// Command server runs the accessible intake gateway HTTP API.
//
// Configuration via environment variables:
//
//	INTAKE_DB    path to SQLite database (default: intake.db)
//	INTAKE_ADDR  listen address (default: :8080)
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

	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/httpapi"
	"github.com/accessible-intake/gateway/internal/intake"
	"github.com/accessible-intake/gateway/internal/store"
)

func main() {
	dbPath := envOr("INTAKE_DB", "intake.db")
	addr := envOr("INTAKE_ADDR", ":8080")

	c := contracts.MustLoad()
	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	svc := intake.New(st, c)
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(svc).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("intake gateway listening on %s (db=%s, version=%s)", addr, dbPath, c.Version())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Println("stopped")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
