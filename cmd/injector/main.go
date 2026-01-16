// cmd/injector/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/deannos/injection-go-pipeline-nuvaris/internal/config"
	"github.com/deannos/injection-go-pipeline-nuvaris/internal/injector"
	"github.com/deannos/injection-go-pipeline-nuvaris/internal/model" // For eventChan type
	"github.com/deannos/injection-go-pipeline-nuvaris/internal/server"
	"go.uber.org/zap"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "./configs", "Path to the configuration directory")
	flag.Parse()

	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() // Flushes any buffered log entries

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err), zap.String("config_path", configPath))
	}
	logger.Info("Configuration loaded successfully", zap.Reflect("config", cfg)) // Be careful logging full config in prod

	// Create the shared event channel
	eventChan := make(chan model.ElectricityBill, cfg.Injector.EventChannelCapacity)
	// The eventChan will be closed by the main function's defer after injector.Stop() is called
	// and workers have had a chance to drain it.
	// defer close(eventChan) // This will be handled more explicitly in Stop sequence

	// Create and start the core injector service
	injectorService, err := injector.NewInjectorService(cfg, logger, eventChan)
	if err != nil {
		logger.Fatal("Failed to create injector service", zap.Error(err))
	}
	injectorService.Start()
	// defer injectorService.Stop(context.Background()) // Ensure Stop is called

	// Create and start the HTTP server
	httpServer := server.NewHTTPServer(cfg, injectorService, logger) // Pass logger here too if server needs it
	if err := httpServer.Start(); err != nil {
		logger.Fatal("Failed to start HTTP server", zap.Error(err))
	}
	// defer httpServer.Stop(context.Background()) // Ensure Stop is called

	logger.Info("Injector Service started successfully")

	// Wait for interrupt signal to gracefully shut down the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // Block until a signal is received
	logger.Info("Shutting down server...")

	// Create a context with a timeout for the shutdown process
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.IdleTimeout+cfg.Server.WriteTimeout+5*time.Second) // Use a reasonable timeout
	defer shutdownCancel()

	// Stop services in reverse order of start
	// 1. Stop HTTP server first to stop accepting new requests
	logger.Info("Stopping HTTP server...")
	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Error during HTTP server shutdown", zap.Error(err))
	} else {
		logger.Info("HTTP server stopped.")
	}

	// 2. Close eventChan after HTTP server is stopped and no more new events will come.
	// This signals the injector workers to drain the channel and exit.
	logger.Info("Closing event channel...")
	close(eventChan) // Close the channel so workers can exit their range loops
	logger.Info("Event channel closed.")

	// 3. Stop the injector service (waits for workers to process remaining events in eventChan and retryChan)
	logger.Info("Stopping injector service...")
	//if err := injectorService.Stop(shutdownCtx); err != nil { // Pass shutdownCtx
	//	logger.Error("Error during injector service shutdown", zap.Error(err))
	//} else {
	//	logger.Info("Injector service stopped.")
	//}
	injectorService.Stop()
	logger.Info("Injector service stopped.")

	logger.Info("Server exited")
}
