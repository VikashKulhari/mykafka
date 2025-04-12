package mykafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// KafkaClient represents a Kafka client that can produce and consume messages.
type KafkaClient struct {
	config      *Config
	writer      *kafka.Writer
	reader      *kafka.Reader // Now initialized in Listen()
	redisClient *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewKafkaClient initializes a new Kafka client with the given configuration.
func NewKafkaClient(config *Config) (*KafkaClient, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &KafkaClient{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := c.initRedis(); err != nil {
		return nil, fmt.Errorf("redis initialization failed: %w", err)
	}

	if config.ProducerRequired {
		if err := c.initProducer(); err != nil {
			return nil, fmt.Errorf("producer initialization failed: %w", err)
		}
	}

	// Consumer is now initialized in Listen()
	return c, nil
}

// initRedis initializes the Redis client with the specified configuration.
func (c *KafkaClient) initRedis() error {
	c.redisClient = redis.NewClient(&redis.Options{
		Addr:     c.config.RedisAddr,
		Password: c.config.RedisPassword,
		DB:       0,
	})
	return c.redisClient.Ping(c.ctx).Err()
}

// initProducer initializes the Kafka producer with the specified configuration.
func (c *KafkaClient) initProducer() error {
	c.writer = &kafka.Writer{
		Addr:         kafka.TCP(c.config.KafkaBrokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return nil
}

// initConsumer initializes the Kafka consumer with the specified configuration.
func (c *KafkaClient) initConsumer(topics []string) error {
	if len(topics) == 0 {
		return errors.New("cannot initialize consumer without topics")
	}
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.config.KafkaBrokers,
		GroupID:     c.config.ConsumerGroupID,
		GroupTopics: topics, // Set topics during initialization
		MaxBytes:    10e6,   // 10MB
	})
	return nil
}

// Close closes the Kafka client, including the producer, consumer, and Redis client.
func (c *KafkaClient) Close() error {
	c.cancel()

	if c.writer != nil {
		if err := c.writer.Close(); err != nil {
			log.Printf("Error closing writer: %v", err)
		}
	}

	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			log.Printf("Error closing reader: %v", err)
		}
	}

	if err := c.redisClient.Close(); err != nil {
		log.Printf("Error closing Redis: %v", err)
	}

	return nil
}
