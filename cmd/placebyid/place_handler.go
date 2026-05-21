package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/scrapemateapp"
)

// ====================================================================
// In-memory ResultWriter for HTTP server mode
// ====================================================================

type memWriter struct {
	ch chan *googlePlace
}

func (m *memWriter) Run(_ context.Context, in <-chan scrapemate.Result) error {
	for result := range in {
		job, ok := result.Job.(scrapemate.IJob)
		if !ok {
			continue
		}
		pid := job.GetParentID()
		if pid == "" {
			pid = job.GetID()
		}
		entries, _ := asEntries(result.Data)
		for _, e := range entries {
			select {
			case m.ch <- convertEntry(e, pid):
			default:
			}
		}
	}
	return nil
}

// buildPlaceURL converts a place ID to the right Google Maps URL.
// Handles three formats:
//   - ChIJ...   → place_id: navigation
//   - 0xHEX:0xHEX  → CID decimal navigation (DataID format from SearchJob)
//   - anything else → place_id: navigation (fallback)
func buildPlaceURL(placeID string) string {
	if strings.HasPrefix(placeID, "0x") && strings.Contains(placeID, ":") {
		parts := strings.SplitN(placeID, ":", 2)
		hexCID := strings.TrimPrefix(parts[1], "0x")
		if cid, err := strconv.ParseUint(hexCID, 16, 64); err == nil {
			return fmt.Sprintf("https://maps.google.com/?cid=%d", cid)
		}
	}
	return fmt.Sprintf("https://www.google.com/maps/place/?q=%s",
		url.QueryEscape("place_id:"+placeID))
}

// ====================================================================
// GET /v1/places/{placeId} handler
// ====================================================================

func placeHandler(concurrency int, langCode string, extractEmail, extraReviews bool, proxies string, inactivity time.Duration) http.HandlerFunc {
	sem := make(chan struct{}, concurrency)
	return func(w http.ResponseWriter, r *http.Request) {
		placeID := r.PathValue("placeId")
		if placeID == "" {
			http.Error(w, "missing placeId", http.StatusBadRequest)
			return
		}

		// Enforce concurrency cap — return 429 if all slots busy.
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			http.Error(w, "too many concurrent requests", http.StatusTooManyRequests)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()

		mem := &memWriter{ch: make(chan *googlePlace, 1)}

		opts := []func(*scrapemateapp.Config) error{
			scrapemateapp.WithConcurrency(1),
			scrapemateapp.WithExitOnInactivity(inactivity),
			scrapemateapp.WithJS(scrapemateapp.DisableImages()),
			scrapemateapp.WithPageReuseLimit(2),
		}
		if proxies != "" {
			opts = append(opts, scrapemateapp.WithProxies(strings.Split(proxies, ",")))
		}

		matecfg, err := scrapemateapp.NewConfig([]scrapemate.ResultWriter{mem}, opts...)
		if err != nil {
			log.Printf("[%s] config: %v", placeID, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		app, err := scrapemateapp.NewScrapeMateApp(matecfg)
		if err != nil {
			log.Printf("[%s] app: %v", placeID, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		defer app.Close()

		u := buildPlaceURL(placeID)
		job := gmaps.NewPlaceJob(placeID, langCode, u, extractEmail, extraReviews)

		if err := app.Start(ctx, job); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			log.Printf("[%s] scrape: %v", placeID, err)
			http.Error(w, "scrape error", http.StatusInternalServerError)
			return
		}

		select {
		case gp := <-mem.ch:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(gp)
		default:
			http.Error(w, "place not found", http.StatusNotFound)
		}
	}
}
