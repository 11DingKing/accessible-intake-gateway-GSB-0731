package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/accessible-intake/gateway/internal/adapter"
	"github.com/accessible-intake/gateway/internal/api"
	"github.com/accessible-intake/gateway/internal/contracts"
	"github.com/accessible-intake/gateway/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "intake.db", "SQLite database path (repeatable: schema is created on startup)")
	contractsPath := flag.String("contracts", "materials/channel-contracts.json", "path to channel contracts JSON")
	flag.Parse()

	c, err := contracts.Load(*contractsPath)
	if err != nil {
		log.Fatalf("load contracts: %v", err)
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rec := adapter.NewRecorder()
	srv := api.NewServer(st, c, rec)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("accessible intake gateway listening on %s (db=%s, version=%s)", *addr, *dbPath, c.CanonicalVersion)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "shutdown error:", err)
	}
}
