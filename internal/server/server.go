// internal/server/server.go
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/deannos/injection-go-pipeline-nuvaris/internal/config"
	"github.com/deannos/injection-go-pipeline-nuvaris/internal/model"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// InjectorService defines the interface for our core injection logic.
// This allows the HTTP server to be decoupled from the specific implementation.
type InjectorService interface {
	IngestBill(ctx context.Context, bill model.ElectricityBill) error // Or IngestBills([]model.ElectricityBill)
	GetMetrics() map[string]interface{}                               // For /metrics endpoint if not using Prometheus client directly
}

// HTTPServer encapsulates the HTTP server and its dependencies.
type HTTPServer struct {
	server      *http.Server
	config      *config.Config
	injector    InjectorService
	rateLimiter *rate.Limiter
	// eventChan    chan model.ElectricityBill // Shared with injector service
	shutdownWg   sync.WaitGroup
	shutdownChan chan struct{}
	logger       *zap.Logger
}

// NewHTTPServer creates a new HTTPServer instance.
// func NewHTTPServer(cfg *config.Config, injector InjectorService, eventChan chan model.ElectricityBill) *HTTPServer {
func NewHTTPServer(cfg *config.Config, injector InjectorService, logger *zap.Logger) *HTTPServer {
	// Rate limiter: cfg.Injector.RateLimitPerSecond requests per second, with a burst of e.g., 2x the rate
	limiter := rate.NewLimiter(rate.Limit(cfg.Injector.RateLimitPerSecond), cfg.Injector.RateLimitPerSecond*2)

	mux := http.NewServeMux()
	srv := &HTTPServer{
		config:       cfg,
		injector:     injector,
		rateLimiter:  limiter,
		logger:       logger,
		shutdownChan: make(chan struct{}),
	}

	srv.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	mux.HandleFunc("/api/v1/bills", srv.handleInjectBills)
	mux.HandleFunc("/health", srv.handleHealthCheck)
	if cfg.Metrics.Enabled {
		// If using Prometheus client, it would typically handle its own /metrics endpoint
		// For simplicity, we'll have a basic one here or assume Prometheus client registers it.
		// mux.HandleFunc(cfg.Metrics.Path, srv.handleMetrics) // Placeholder
	}

	return srv
}

// Start starts the HTTP server.
func (s *HTTPServer) Start() error {
	s.shutdownWg.Add(1)
	go func() {
		defer s.shutdownWg.Done()
		s.logger.Info("HTTP server starting",
			zap.Int("port", s.config.Server.Port),
		)

		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Fatal("HTTP server failed to start", zap.Error(err))
		}
	}()
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPServer) Stop(ctx context.Context) error {
	fmt.Println("Shutting down HTTP server...")
	close(s.shutdownChan) // Signal other goroutines if needed
	err := s.server.Shutdown(ctx)
	s.shutdownWg.Wait()
	fmt.Println("HTTP server stopped.")
	return err
}

// handleInjectBills handles POST requests to /api/v1/bills.
func (s *HTTPServer) handleInjectBills(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Rate limiting
	if !s.rateLimiter.Allow() {
		http.Error(w, "Rate limit exceeded. Please try again later.", http.StatusTooManyRequests)
		return
	}

	var bills []model.ElectricityBill
	if err := json.NewDecoder(r.Body).Decode(&bills); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON payload: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(bills) == 0 {
		http.Error(w, "No bills provided in the request", http.StatusBadRequest)
		return
	}

	acceptedCount := 0
	for _, bill := range bills {
		if err := bill.Validate(); err != nil {
			// Log validation error for this specific bill but continue with others in the batch
			fmt.Printf("Validation failed for bill ID %s: %v\n", bill.BillID, err)
			// Optionally, increment a metric for validation errors
			continue
		}

		// Set ingestion timestamp if not already set by client (though client shouldn't send it)
		if bill.IngestedAt.IsZero() {
			bill.IngestedAt = time.Now().UTC()
		}

		//select {
		//case s.eventChan <- bill:
		//	acceptedCount++
		//default:
		//	// Event channel is full, service is overloaded.
		//	// Log this event, potentially increment a metric.
		//	fmt.Printf("Event channel full. Dropped bill ID %s. Current channel size approx %d/%d\n",
		//		bill.BillID, len(s.eventChan), cap(s.eventChan))
		//	// If any bill in the batch cannot be enqueued, we could choose to:
		//	// 1. Reject the entire batch (simpler for client to handle).
		//	// 2. Accept what we can and report partial success (more complex).
		//	// For now, if one fails due to full channel, we stop processing this batch and notify.
		//	// To accept partial batches, we'd need to continue the loop and report status differently.
		//	http.Error(w, "Service overloaded. Event channel full. Please retry.", http.StatusServiceUnavailable)
		//	return
		//}
		if err := s.injector.IngestBill(r.Context(), bill); err != nil {
			http.Error(
				w,
				"Service overloaded. Please retry.",
				http.StatusServiceUnavailable,
			)
			return
		}
		acceptedCount++

	}

	w.WriteHeader(http.StatusAccepted)
	// Optionally, write a response body, e.g., {"accepted": acceptedCount, "rejected": len(bills) - acceptedCount}
	// For high throughput, minimizing response body size can be beneficial.
	response := map[string]int{"accepted": acceptedCount, "received": len(bills)}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Printf("Error encoding response: %v\n", err) // Log error, but headers might already be sent.
	}
}

// handleHealthCheck handles GET requests to /health.
func (s *HTTPServer) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy", "timestamp": time.Now().UTC().Format(time.RFC3339)})
}

// handleMetrics (Placeholder) - would typically be handled by Prometheus client
// func (s *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodGet {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		return
// 	}
// 	// s.injector.GetMetrics() would return metrics to be formatted
// 	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
// 	w.WriteHeader(http.StatusOK)
// 	// fmt.Fprintf(w, "# HELP injector_events_accepted_total Number of events accepted by the injector.\n")
// 	// fmt.Fprintf(w, "# TYPE injector_events_accepted_total counter\n")
// 	// fmt.Fprintf(w, "injector_events_accepted_total %d\n", s.injector.GetAcceptedCount()) // Example
// }
