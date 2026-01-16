// internal/injector/service.go
package injector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/deannos/injection-go-pipeline-nuvaris/internal/config"
	"github.com/deannos/injection-go-pipeline-nuvaris/internal/model"
	"go.uber.org/zap" // For structured logging
)

// InjectorService implements the core logic for ingesting bills and sending them to Kafka.
type InjectorService struct {
	config         *config.Config
	logger         *zap.Logger
	eventChan      chan model.ElectricityBill
	retryChan      chan *sarama.ProducerMessage
	kafkaProducer  sarama.AsyncProducer
	workerWg       sync.WaitGroup
	retryWorkerWg  sync.WaitGroup
	shutdownChan   chan struct{}
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Metrics (simple counters for now, could be Prometheus counters)
	mu               sync.Mutex
	eventsAccepted   uint64
	eventsDropped    uint64
	kafkaErrors      uint64
	kafkaSuccesses   uint64
	retriesAttempted uint64
	dlqMessagesSent  uint64
}

// NewInjectorService creates a new InjectorService.
func NewInjectorService(cfg *config.Config, logger *zap.Logger, eventChan chan model.ElectricityBill) (*InjectorService, error) {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	service := &InjectorService{
		config:         cfg,
		logger:         logger,
		eventChan:      eventChan,
		retryChan:      make(chan *sarama.ProducerMessage, cfg.Injector.Retry.ChannelCapacity),
		shutdownChan:   make(chan struct{}),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}

	kafkaProducer, err := service.setupKafkaProducer()
	if err != nil {
		logger.Error("Failed to setup Kafka producer", zap.Error(err))
		return nil, fmt.Errorf("failed to setup Kafka producer: %w", err)
	}
	service.kafkaProducer = kafkaProducer

	return service, nil
}

// setupKafkaProducer configures and creates a Sarama AsyncProducer.
func (s *InjectorService) setupKafkaProducer() (sarama.AsyncProducer, error) {
	saramaConfig := sarama.NewConfig()

	// Producer settings from Viper config
	// saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(s.config.Kafka.Producer.RequiredAcks) as it return type error
	acks, err := parseRequiredAcks(s.config.Kafka.Producer.RequiredAcks)
	if err != nil {
		return nil, err
	}
	saramaConfig.Producer.RequiredAcks = acks

	switch s.config.Kafka.Producer.CompressionCodec {
	case "none":
		saramaConfig.Producer.Compression = sarama.CompressionNone
	case "gzip":
		saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		saramaConfig.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		saramaConfig.Producer.Compression = sarama.CompressionZSTD
	default:
		s.logger.Warn("Unknown compression codec, defaulting to Snappy", zap.String("codec", s.config.Kafka.Producer.CompressionCodec))
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	}

	saramaConfig.Producer.Flush.Frequency = s.config.Kafka.Producer.FlushFrequency
	saramaConfig.Producer.Flush.Messages = s.config.Kafka.Producer.FlushMessages
	saramaConfig.Producer.Flush.Bytes = s.config.Kafka.Producer.FlushBytes
	saramaConfig.Producer.Retry.Max = s.config.Kafka.Producer.RetryMax
	saramaConfig.Producer.Retry.Backoff = s.config.Kafka.Producer.RetryBackoff
	saramaConfig.Producer.Return.Successes = s.config.Kafka.Producer.ReturnSuccesses
	saramaConfig.Producer.Return.Errors = s.config.Kafka.Producer.ReturnErrors
	// saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner // Default

	producer, err := sarama.NewAsyncProducer(s.config.Kafka.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating Sarama AsyncProducer: %w", err)
	}

	// Start goroutines to handle producer success/error messages
	go s.handleProducerNotifications(producer)

	return producer, nil
}

// Start initializes and starts the worker pools.
func (s *InjectorService) Start() {
	s.logger.Info("Starting injector service...",
		zap.Int("num_workers", s.config.Injector.NumWorkers),
		zap.Int("event_channel_capacity", cap(s.eventChan)),
		zap.Int("retry_channel_capacity", cap(s.retryChan)),
		zap.Int("num_retry_workers", s.config.Injector.Retry.NumWorkers),
	)

	// Start main worker pool
	for i := 0; i < s.config.Injector.NumWorkers; i++ {
		s.workerWg.Add(1)
		go s.worker(i)
	}

	// Start retry worker pool
	for i := 0; i < s.config.Injector.Retry.NumWorkers; i++ {
		s.retryWorkerWg.Add(1)
		go s.retryWorker(i)
	}
	s.logger.Info("Injector service started.")
}

// Stop gracefully shuts down the injector service.
func (s *InjectorService) Stop() {
	s.logger.Info("Stopping injector service...")
	s.shutdownCancel() // Signal shutdown to all goroutines

	close(s.shutdownChan) // Alternative/Additional shutdown signal if needed

	// Close eventChan to signal workers no more new bills will arrive.
	// This should be done after ensuring HTTP server is no longer writing to it.
	// If Stop() is called after HTTP server is stopped, this is safe.
	// close(s.eventChan) // This is done by the caller (main.go) after HTTP server stops.

	s.workerWg.Wait() // Wait for main workers to drain eventChan
	s.logger.Info("Main workers stopped.")

	close(s.retryChan)     // Close retryChan after main workers are done
	s.retryWorkerWg.Wait() // Wait for retry workers to drain retryChan
	s.logger.Info("Retry workers stopped.")

	if err := s.kafkaProducer.Close(); err != nil {
		s.logger.Error("Error closing Kafka producer", zap.Error(err))
	} else {
		s.logger.Info("Kafka producer closed.")
	}
	s.logger.Info("Injector service stopped.")
}

// worker processes bills from the eventChan and sends them to Kafka.
func (s *InjectorService) worker(id int) {
	defer s.workerWg.Done()
	s.logger.Info("Worker started", zap.Int("worker_id", id))

	for {
		select {
		case bill, ok := <-s.eventChan:
			if !ok { // Channel closed
				s.logger.Info("Event channel closed, worker exiting", zap.Int("worker_id", id))
				return
			}
			s.processBill(bill)
		case <-s.shutdownCtx.Done(): // Context cancelled
			s.logger.Info("Shutdown signal received, worker exiting", zap.Int("worker_id", id))
			// Drain remaining events from eventChan before exiting if possible
			// This simple select will exit immediately. For a more graceful drain,
			// one might loop on eventChan until it's empty after shutdown signal.
			return
		}
	}
}

// processBill converts a bill to a Kafka message and sends it to the producer.
func (s *InjectorService) processBill(bill model.ElectricityBill) {
	value, err := json.Marshal(bill)
	if err != nil {
		s.logger.Error("Failed to marshal bill to JSON",
			zap.String("bill_id", bill.BillID),
			zap.Error(err),
		)
		s.incrementEventsDropped()
		return
	}

	// Use CustomerID as the key for partitioning.
	// This ensures all bills for a specific customer go to the same partition,
	// maintaining order for that customer if needed by downstream consumers.
	key := sarama.StringEncoder(bill.CustomerID)

	kafkaMessage := &sarama.ProducerMessage{
		Topic: s.config.Kafka.Topic,
		Key:   key,
		Value: sarama.ByteEncoder(value),
		// Metadata can be used to store application-specific info, e.g., original bill ID for tracing
	}

	select {
	case s.kafkaProducer.Input() <- kafkaMessage:
		s.incrementEventsAccepted()
		s.logger.Debug("Bill sent to Kafka producer input", zap.String("bill_id", bill.BillID))
	default:
		// Producer's input channel is full. This indicates Kafka is likely slow or down.
		s.logger.Warn("Kafka producer input channel full. Dropping bill.",
			zap.String("bill_id", bill.BillID),
			zap.String("topic", s.config.Kafka.Topic),
		)
		s.incrementEventsDropped()
		// This is a critical point. Consider a more robust strategy than just dropping,
		// e.g., a short-lived local queue or more aggressive backpressure.
		// However, if the producer's own internal buffer is full, it's a severe issue.
	}
}

func (s *InjectorService) IngestBill(ctx context.Context, bill model.ElectricityBill) error {
	select {
	case s.eventChan <- bill:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.shutdownCtx.Done():
		return fmt.Errorf("injector shutting down")
	default:
		// backpressure protection
		s.incrementEventsDropped()
		return fmt.Errorf("event channel full")
	}
}

// handleProducerNotifications processes success and error messages from the Kafka producer.
func (s *InjectorService) handleProducerNotifications(producer sarama.AsyncProducer) {
	for {
		select {
		case success, ok := <-producer.Successes():
			if !ok {
				s.logger.Info("Producer successes channel closed.")
				return
			}
			if s.config.Kafka.Producer.ReturnSuccesses {
				s.incrementKafkaSuccesses()
				s.logger.Debug("Message successfully sent to Kafka",
					zap.String("topic", success.Topic),
					zap.Int32("partition", success.Partition),
					zap.Int64("offset", success.Offset),
					// zap.Any("key", success.Key), // Key can be various types
				)
			}
		case err, ok := <-producer.Errors():
			if !ok {
				s.logger.Info("Producer errors channel closed.")
				return
			}
			s.incrementKafkaErrors()
			s.logger.Error("Failed to produce message to Kafka",
				zap.String("topic", err.Msg.Topic),
				zap.Error(err.Err),
				// zap.Any("key", err.Msg.Key),
			)
			s.sendToRetryQueue(err.Msg)
		case <-s.shutdownCtx.Done():
			s.logger.Info("Shutdown signal received in producer notification handler.")
			return
		}
	}
}

// sendToRetryQueue sends a failed Kafka message to the retry channel.
func (s *InjectorService) sendToRetryQueue(msg *sarama.ProducerMessage) {
	select {
	case s.retryChan <- msg:
		s.logger.Debug("Message sent to retry queue", zap.String("topic", msg.Topic))
	default:
		// Retry channel is also full. This is a critical failure path.
		s.logger.Error("Retry queue full. Dropping failed message.",
			zap.String("topic", msg.Topic),
			// zap.Any("key", msg.Key),
		)
		s.incrementEventsDropped() // Or a specific "dlq_dropped" metric
	}
}

// retryWorker processes messages from the retryChan.
func (s *InjectorService) retryWorker(id int) {
	defer s.retryWorkerWg.Done()
	s.logger.Info("Retry worker started", zap.Int("retry_worker_id", id))

	for {
		select {
		case msg, ok := <-s.retryChan:
			if !ok { // Channel closed
				s.logger.Info("Retry channel closed, retry worker exiting", zap.Int("retry_worker_id", id))
				return
			}
			s.retryMessage(msg)
		case <-s.shutdownCtx.Done():
			s.logger.Info("Shutdown signal received, retry worker exiting", zap.Int("retry_worker_id", id))
			return
		}
	}
}

// retryMessage attempts to resend a message to Kafka with exponential backoff.
func (s *InjectorService) retryMessage(msg *sarama.ProducerMessage) {
	retryCount, ok := msg.Metadata.(int)
	if !ok {
		retryCount = 0 // Initialize if not present or not an int
	}

	if retryCount >= s.config.Injector.Retry.MaxRetries {
		s.logger.Warn("Message exceeded max retry attempts. Sending to DLQ.",
			zap.String("topic", msg.Topic),
			zap.Int("retry_count", retryCount),
			// zap.Any("key", msg.Key),
		)
		s.sendToDLQ(msg)
		return
	}

	backoff := time.Duration(float64(s.config.Injector.Retry.InitialBackoff) * math.Pow(s.config.Injector.Retry.BackoffMultiplier, float64(retryCount)))
	if backoff > s.config.Injector.Retry.MaxBackoff {
		backoff = s.config.Injector.Retry.MaxBackoff
	}
	// Add jitter to prevent thundering herd if multiple retry workers backoff at similar intervals
	jitter := time.Duration(rand.Int63n(int64(backoff / 10))) // Up to 10% jitter
	backoff += jitter

	s.logger.Info("Retrying message",
		zap.String("topic", msg.Topic),
		zap.Int("attempt", retryCount+1),
		zap.Duration("backoff", backoff),
		// zap.Any("key", msg.Key),
	)

	select {
	case <-time.After(backoff):
		msg.Metadata = retryCount + 1 // Update retry count
		select {
		case s.kafkaProducer.Input() <- msg:
			s.incrementRetriesAttempted()
			s.logger.Debug("Message re-sent to Kafka producer input from retry worker", zap.String("topic", msg.Topic))
		default:
			// Producer input still full, re-queue to retryChan
			s.logger.Warn("Kafka producer input full during retry. Re-queuing.", zap.String("topic", msg.Topic))
			s.sendToRetryQueue(msg) // This will increment retry count again if it goes through
		}
	case <-s.shutdownCtx.Done():
		s.logger.Info("Shutdown received during message retry, abandoning.", zap.String("topic", msg.Topic))
		// Optionally, send to DLQ if shutdown happens during retry
		// s.sendToDLQ(msg)
		return
	}
}

// sendToDLQ publishes a persistently failed message to the Dead-Letter Queue.
func (s *InjectorService) sendToDLQ(msg *sarama.ProducerMessage) {
	var keyStr string
	if enc, ok := msg.Key.(sarama.Encoder); ok {
		if b, err := enc.Encode(); err == nil {
			keyStr = string(b)
		}
	}

	dlqMessage := &sarama.ProducerMessage{
		Topic: s.config.Kafka.DLQTopic,
		Key:   msg.Key,   // Preserve original key
		Value: msg.Value, // Preserve original value
		// Metadata could store original topic, error, retry count etc.
		Metadata: map[string]interface{}{
			"original_topic": msg.Topic,
			// "original_key_str":  string(msg.Key.(sarama.Encoder).Encode()), // Ensure key is string for DLQ metadata but here multiple value in single-value context
			// need to fix
			"original_key_str":  keyStr,
			"final_retry_count": msg.Metadata,
			"dlq_timestamp":     time.Now().UTC(),
		},
	}

	select {
	case s.kafkaProducer.Input() <- dlqMessage:
		s.incrementDLQMessagesSent()
		s.logger.Info("Message sent to DLQ",
			zap.String("dlq_topic", s.config.Kafka.DLQTopic),
			zap.String("original_topic", msg.Topic),
		)
	default:
		s.logger.Error("CRITICAL: Kafka producer input channel full. FAILED to send message to DLQ. DATA MAY BE LOST.",
			zap.String("dlq_topic", s.config.Kafka.DLQTopic),
			zap.String("original_topic", msg.Topic),
		)
	}
}

// Metric incrementors (simple for now, replace with Prometheus/Grafana)
func (s *InjectorService) incrementEventsAccepted() { s.mu.Lock(); s.eventsAccepted++; s.mu.Unlock() }
func (s *InjectorService) incrementEventsDropped()  { s.mu.Lock(); s.eventsDropped++; s.mu.Unlock() }
func (s *InjectorService) incrementKafkaErrors()    { s.mu.Lock(); s.kafkaErrors++; s.mu.Unlock() }
func (s *InjectorService) incrementKafkaSuccesses() { s.mu.Lock(); s.kafkaSuccesses++; s.mu.Unlock() }
func (s *InjectorService) incrementRetriesAttempted() {
	s.mu.Lock()
	s.retriesAttempted++
	s.mu.Unlock()
}
func (s *InjectorService) incrementDLQMessagesSent() { s.mu.Lock(); s.dlqMessagesSent++; s.mu.Unlock() }

// GetMetrics returns current metric values (for simple /metrics endpoint)
func (s *InjectorService) GetMetrics() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]interface{}{
		"events_accepted_total":         s.eventsAccepted,
		"events_dropped_total":          s.eventsDropped,
		"kafka_produce_errors_total":    s.kafkaErrors,
		"kafka_produce_successes_total": s.kafkaSuccesses,
		"retries_attempted_total":       s.retriesAttempted,
		"dlq_messages_sent_total":       s.dlqMessagesSent,
	}
}
