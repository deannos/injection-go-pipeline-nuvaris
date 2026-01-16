// cmd/datagen/main.go
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/deannos/injection-go-pipeline-nuvaris/internal/model" // Reuse the model
	"github.com/google/uuid"                                          // For generating unique IDs
)

// Config for the data generator
type GenConfig struct {
	TargetURL         string
	NumClients        int
	RequestsPerClient int
	BillsPerRequest   int
	RatePerSecond     int // Approximate overall rate
}

// generateRandomBill creates a single, randomized ElectricityBill.
func generateRandomBill() model.ElectricityBill {
	now := time.Now().UTC()
	billPeriodStart := now.AddDate(0, -1, 0)          // Approx 1 month ago
	billPeriodEnd := now.AddDate(0, 0, -rand.Intn(5)) // End within last 5 days

	prevReading := rand.Float64() * 10000              // Random previous reading
	currentReading := prevReading + rand.Float64()*500 // Random current reading, higher than prev
	consumption := currentReading - prevReading

	tariffTypes := []string{"residential", "commercial", "industrial"}
	tariffType := tariffTypes[rand.Intn(len(tariffTypes))]
	ratePerKWh := 0.10 + rand.Float64()*0.20 // Rate between 0.10 and 0.30

	energyCharge := consumption * ratePerKWh
	fixedCharges := 5.0 + rand.Float64()*15.0                             // Fixed charges between 5 and 20
	taxes := (energyCharge + fixedCharges) * (0.05 + rand.Float64()*0.10) // Taxes 5-15%
	totalAmount := energyCharge + fixedCharges + taxes

	paymentStatuses := []string{"unpaid", "paid", "overdue"}
	paymentStatus := paymentStatuses[rand.Intn(len(paymentStatuses))]

	return model.ElectricityBill{
		BillID:                 uuid.New().String(),
		CustomerID:             uuid.New().String(),
		AccountNumber:          fmt.Sprintf("ACC-%d", rand.Intn(1000000)),
		ServiceAddress:         fmt.Sprintf("%d Main St, Anytown, USA", rand.Intn(1000)),
		BillingPeriodStartDate: billPeriodStart,
		BillingPeriodEndDate:   billPeriodEnd,
		MeterReadingPrevious:   prevReading,
		MeterReadingCurrent:    currentReading,
		ConsumptionKWh:         consumption,
		TariffType:             tariffType,
		RatePerKWh:             ratePerKWh,
		EnergyCharge:           energyCharge,
		FixedCharges:           fixedCharges,
		Taxes:                  taxes,
		TotalAmountDue:         totalAmount,
		DueDate:                now.AddDate(0, 0, rand.Intn(30)+1), // Due within next 30 days
		BillIssueDate:          billPeriodEnd.AddDate(0, 0, 1),     // Issued a day after period ends
		PaymentStatus:          paymentStatus,
	}
}

// clientWorker simulates a client sending requests.
func clientWorker(id int, cfg GenConfig, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	client := &http.Client{
		Timeout: 30 * time.Second, // Timeout for requests
		// Consider adding Transport settings for connection pooling if making many requests
	}

	for i := 0; i < cfg.RequestsPerClient; i++ {
		var bills []model.ElectricityBill
		for j := 0; j < cfg.BillsPerRequest; j++ {
			bills = append(bills, generateRandomBill())
		}

		jsonPayload, err := json.Marshal(bills)
		if err != nil {
			results <- fmt.Sprintf("Client %d, Request %d: JSON marshal error: %v", id, i+1, err)
			continue
		}

		req, err := http.NewRequest("POST", cfg.TargetURL, bytes.NewBuffer(jsonPayload))
		if err != nil {
			results <- fmt.Sprintf("Client %d, Request %d: Error creating request: %v", id, i+1, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			results <- fmt.Sprintf("Client %d, Request %d: HTTP request error: %v", id, i+1, err)
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body) // Read body even on success for potential logging

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			results <- fmt.Sprintf("Client %d, Request %d: Success (%d) - Sent %d bills", id, i+1, resp.StatusCode, cfg.BillsPerRequest)
		} else {
			results <- fmt.Sprintf("Client %d, Request %d: Failed (%d) - Response: %s", id, i+1, resp.StatusCode, string(body))
		}

		if cfg.RatePerSecond > 0 {
			// Simple rate limiting per client
			// For more accurate overall rate, a token bucket shared across clients would be better
			time.Sleep(time.Duration(float64(time.Second) * float64(cfg.NumClients*cfg.BillsPerRequest) / float64(cfg.RatePerSecond)))
		}
	}
}

func main() {
	var cfg GenConfig
	flag.StringVar(&cfg.TargetURL, "url", "http://localhost:8080/api/v1/bills", "Target URL for injection service")
	flag.IntVar(&cfg.NumClients, "clients", 10, "Number of concurrent client goroutines")
	flag.IntVar(&cfg.RequestsPerClient, "requests", 100, "Number of requests per client")
	flag.IntVar(&cfg.BillsPerRequest, "batch", 100, "Number of bills per request (batch size)")
	flag.IntVar(&cfg.RatePerSecond, "rate", 0, "Target overall send rate in bills/sec (approximate, 0 for no rate limit)")
	flag.Parse()

	if cfg.RatePerSecond > 0 && cfg.NumClients*cfg.BillsPerRequest > cfg.RatePerSecond {
		fmt.Printf("Warning: Max possible rate (%d bills/sec) is less than configured rate (%d bills/sec). Rate limiting will be per client.\n", cfg.NumClients*cfg.BillsPerRequest, cfg.RatePerSecond)
	}

	fmt.Printf("Starting data generator with config:\n")
	fmt.Printf("  Target URL: %s\n", cfg.TargetURL)
	fmt.Printf("  Clients: %d\n", cfg.NumClients)
	fmt.Printf("  Requests/Client: %d\n", cfg.RequestsPerClient)
	fmt.Printf("  Bills/Request: %d\n", cfg.BillsPerRequest)
	fmt.Printf("  Total Bills to Send: %d\n", cfg.NumClients*cfg.RequestsPerClient*cfg.BillsPerRequest)
	fmt.Printf("  Target Rate: %d bills/sec\n", cfg.RatePerSecond)
	fmt.Println("-------------------------------------")

	startTime := time.Now()
	var wg sync.WaitGroup
	results := make(chan string, cfg.NumClients*cfg.RequestsPerClient) // Buffered channel

	for i := 0; i < cfg.NumClients; i++ {
		wg.Add(1)
		go clientWorker(i+1, cfg, &wg, results)
	}

	// Close results channel once all clients are done
	go func() {
		wg.Wait()
		close(results)
	}()

	var successCount, failureCount, totalBillsSent int
	for res := range results {
		fmt.Println(res) // Print each result
		// A more robust solution would parse the result string for status codes
		// For simplicity, we'll just count.
		if strings.Contains(res, "Success (") {
			successCount++
			totalBillsSent += cfg.BillsPerRequest
		} else {
			failureCount++
		}

	}

	duration := time.Since(startTime)
	fmt.Println("-------------------------------------")
	fmt.Printf("Data generation finished.\n")
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Successful Requests: %d\n", successCount)
	fmt.Printf("Failed Requests: %d\n", failureCount)
	fmt.Printf("Total Bills (approx, if all successful): %d\n", totalBillsSent)
	if duration.Seconds() > 0 {
		fmt.Printf("Approximate Throughput: %.2f bills/sec\n", float64(totalBillsSent)/duration.Seconds())
	}
}
