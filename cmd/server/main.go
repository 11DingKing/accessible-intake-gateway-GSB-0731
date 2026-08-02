// Command server runs the accessible intake gateway: one canonical request
// model fed by physical-site, hotline and web adapters, backed by SQLite.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"accessible-intake-gateway/internal/gateway"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	var (
		addr         = flag.String("addr", envOr("ADDR", ":8080"), "listen address")
		dbDSN        = flag.String("db", envOr("DB_DSN", "file:intake.db?_pragma=busy_timeout(5000)&_txlock=immediate"), "SQLite DSN")
		contractPath = flag.String("contracts", envOr("CONTRACTS", "materials/channel-contracts.json"), "path to channel contract JSON")
	)
	flag.Parse()

	reg, err := gateway.LoadRegistry(*contractPath)
	if err != nil {
		log.Fatalf("load contracts: %v", err)
	}
	store, err := gateway.Open(*dbDSN)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("migrate database: %v", err)
	}

	svc := gateway.NewService(store, reg)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           gateway.NewHandler(svc, reg),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("accessible-intake-gateway listening on %s (canonicalVersion=%s, channels=%v, db=%s)",
			*addr, reg.CanonicalVersion, reg.ChannelIDs(), *dbDSN)
		errCh <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("graceful shutdown: %v", err)
		}
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}
	fmt.Println("server stopped")
}
