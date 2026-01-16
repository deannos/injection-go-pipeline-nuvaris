// internal/config/config.go
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the application.
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	Injector InjectorConfig `mapstructure:"injector"`
	Logging  LoggingConfig  `mapstructure:"logging"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

// ServerConfig defines HTTP server settings.
type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

// KafkaConfig defines Kafka producer settings.
type KafkaConfig struct {
	Brokers  []string       `mapstructure:"brokers"`
	Topic    string         `mapstructure:"topic"`
	DLQTopic string         `mapstructure:"dlq_topic"`
	Producer ProducerConfig `mapstructure:"producer"`
}

// ProducerConfig defines Sarama producer settings.
type ProducerConfig struct {
	RequiredAcks     string        `mapstructure:"required_acks"`
	CompressionCodec string        `mapstructure:"compression_codec"`
	FlushFrequency   time.Duration `mapstructure:"flush_frequency"`
	FlushMessages    int           `mapstructure:"flush_messages"`
	FlushBytes       int           `mapstructure:"flush_bytes"`
	RetryMax         int           `mapstructure:"retry_max"`
	RetryBackoff     time.Duration `mapstructure:"retry_backoff"`
	ReturnSuccesses  bool          `mapstructure:"return_successes"`
	ReturnErrors     bool          `mapstructure:"return_errors"`
	// ChannelBufferSize int           `mapstructure:"channel_buffer_size"`
}

// InjectorConfig defines the injection service's internal settings.
type InjectorConfig struct {
	EventChannelCapacity int         `mapstructure:"event_channel_capacity"`
	NumWorkers           int         `mapstructure:"num_workers"`
	RateLimitPerSecond   int         `mapstructure:"rate_limit_per_second"`
	Retry                RetryConfig `mapstructure:"retry"`
}

// RetryConfig defines settings for the retry mechanism.
type RetryConfig struct {
	ChannelCapacity   int           `mapstructure:"channel_capacity"`
	NumWorkers        int           `mapstructure:"num_workers"`
	MaxRetries        int           `mapstructure:"max_retries"`
	InitialBackoff    time.Duration `mapstructure:"initial_backoff"`
	MaxBackoff        time.Duration `mapstructure:"max_backoff"`
	BackoffMultiplier float64       `mapstructure:"backoff_multiplier"`
}

// LoggingConfig defines logging settings.
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// MetricsConfig defines metrics settings.
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

// LoadConfig loads configuration from file and environment variables.
func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigName("config")          // Name of config file (without extension)
	viper.SetConfigType("yaml")            // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(configPath)        // Path to look for the config file in
	viper.AddConfigPath(".")               // Optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.app-name") // Optionally look for config in the home directory

	// Enable reading from environment variables
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv() // Automatically read from environment variables that match the config structure

	// Set defaults for some values if not provided
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("kafka.producer.required_acks", "leader")
	viper.SetDefault("kafka.producer.compression_codec", "snappy")
	viper.SetDefault("kafka.producer.return_successes", false)
	viper.SetDefault("kafka.producer.return_errors", true)
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("metrics.enabled", true)
	viper.SetDefault("metrics.path", "/metrics")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore if desired, or return error
			fmt.Printf("Config file not found: %s. Using defaults and environment variables.\n", configPath)
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	} else {
		fmt.Printf("Using config file: %s\n", viper.ConfigFileUsed())
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %w", err)
	}

	// Validate critical configuration
	if len(config.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers must be specified")
	}
	if config.Kafka.Topic == "" {
		return nil, fmt.Errorf("kafka topic must be specified")
	}
	if config.Injector.EventChannelCapacity <= 0 {
		return nil, fmt.Errorf("event_channel_capacity must be positive")
	}
	if config.Injector.NumWorkers <= 0 {
		return nil, fmt.Errorf("num_workers must be positive")
	}

	return &config, nil
}
