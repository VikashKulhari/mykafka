package mykafka

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers     []string
	RedisAddr        string
	RedisPassword    string
	RetryInterval    time.Duration
	ConsumerGroupID  string
	ProducerRequired bool
	ConsumerRequired bool
	MaxRetries       int    // New: Maximum number of retry attempts
	RetryQueue       string // New: Redis list name for retries
	DeadLetterQueue  string
	Topics           []string
}

func LoadConfig() *Config {
	return &Config{
		KafkaBrokers:     strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		RedisAddr:        os.Getenv("REDIS_ADDR"),
		RedisPassword:    os.Getenv("REDIS_PASSWORD"),
		RetryInterval:    20 * time.Second,
		ConsumerGroupID:  os.Getenv("KAFKA_CONSUMER_GROUP_ID"),
		ProducerRequired: os.Getenv("KAFKA_PRODUCER_REQUIRED") == "true",
		ConsumerRequired: os.Getenv("KAFKA_CONSUMER_REQUIRED") == "true",
		MaxRetries:       getIntEnv("KAFKA_MAX_RETRIES", 3), // Default 3 retries
		RetryQueue:       "kafka:retry",
		DeadLetterQueue:  "kafka:dead-letter",
		Topics:           strings.Split(os.Getenv("KAFKA_TOPICS"), ","),
	}
}

// Helper function for integer environment variables
func getIntEnv(key string, defaultValue int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultValue
}
