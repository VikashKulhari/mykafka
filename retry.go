package mykafka

import (
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func (c *KafkaClient) StartRetryProcessor() {
	go func() {
		ticker := time.NewTicker(c.config.RetryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				c.processRetries()
			}
		}
	}()
}

func (c *KafkaClient) processRetries() {
	for {
		// Get message from retry queue
		data, err := c.redisClient.LPop(c.ctx, c.config.RetryQueue).Bytes()
		if err == redis.Nil {
			break
		}
		if err != nil {
			log.Printf("Error retrieving failed message: %v", err)
			break
		}

		var msg FailedMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("Error unmarshaling failed message: %v", err)
			continue
		}

		// Check max retries
		if msg.RetryCount >= c.config.MaxRetries {
			log.Printf("Message exceeded max retries (%d) for topic %s",
				c.config.MaxRetries, msg.Topic)

			// Move to dead-letter queue
			if err := c.redisClient.LPush(c.ctx, c.config.DeadLetterQueue, data).Err(); err != nil {
				log.Printf("Failed to move to dead-letter queue: %v", err)
			}
			continue
		}

		// Attempt retry
		if err := c.Publish(msg.Topic, msg.Message); err != nil {
			log.Printf("Retry failed for topic %s (attempt %d/%d): %v",
				msg.Topic, msg.RetryCount+1, c.config.MaxRetries, err)

			// Increment retry count and push back to queue
			msg.RetryCount++
			newData, _ := json.Marshal(msg)
			if err := c.redisClient.LPush(c.ctx, c.config.RetryQueue, newData).Err(); err != nil {
				log.Printf("Failed to re-queue message: %v", err)
			}
		} else {
			log.Printf("Successfully retried message for topic %s (attempt %d/%d)",
				msg.Topic, msg.RetryCount+1, c.config.MaxRetries)
		}
	}
}
