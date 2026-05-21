package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// runServer wires up the two API handlers and runs the HTTP server.
func runServer(port, concurrency int, langCode string, extractEmail, extraReviews bool, proxies string, inactivity time.Duration) {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/places/{placeId}", placeHandler(concurrency, langCode, extractEmail, extraReviews, proxies, inactivity))
	mux.Handle("POST /v1/places:searchText", searchTextHandler(langCode))

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       70 * time.Second,
		WriteTimeout:      70 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx) //nolint:errcheck
	}()

	log.Printf("placebyid server listening on %s  (Ctrl+C to stop)", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}
