package mykafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type FailedMessage struct {
	Topic      string `json:"topic"`
	Message    []byte `json:"message"`
	RetryCount int    `json:"retry_count"`
}

// Publish sends a message to the specified Kafka topic.
func (c *KafkaClient) Publish(topic string, message []byte) error {
	if c.writer == nil {
		return errors.New("producer not initialized")
	}

	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	err := c.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: message,
	})

	if err != nil {
		return c.storeFailedMessage(topic, message, err)
	}
	return nil
}

func (c *KafkaClient) storeFailedMessage(topic string, message []byte, err error) error {
	failedMsg := FailedMessage{
		Topic:      topic,
		Message:    message,
		RetryCount: 0,
	}

	data, marshalErr := json.Marshal(failedMsg)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal message: %w (original error: %v)", marshalErr, err)
	}

	if redisErr := c.redisClient.RPush(c.ctx, "kafka:retry", data).Err(); redisErr != nil {
		return fmt.Errorf("failed to store message in Redis: %w (original error: %v)", redisErr, err)
	}

	return err
}
