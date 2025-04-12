package mykafka

import (
	"context"
	"errors"
	"fmt"
	"log"
)

type MessageHandler func(message []byte) error

// func (c *Client) Listen(topics []string, handler MessageHandler) error {
// 	if c.reader == nil {
// 		return errors.New("consumer not initialized")
// 	}

// 	// Subscribe to topics
// 	c.reader.Config().GroupTopics = topics

// 	go func() {
// 		for {
// 			select {
// 			case <-c.ctx.Done():
// 				return
// 			default:
// 				msg, err := c.reader.ReadMessage(c.ctx)
// 				if err != nil {
// 					log.Printf("Error reading message: %v", err)
// 					continue
// 				}

// 				if err := handler(msg.Value); err != nil {
// 					log.Printf("Error processing message: %v", err)
// 					continue
// 				}

// 				if err := c.reader.CommitMessages(c.ctx, msg); err != nil {
// 					log.Printf("Error committing message: %v", err)
// 				}
// 			}
// 		}
// 	}()

//		return nil
//	}
func (c *KafkaClient) Listen(topics []string, handler MessageHandler) error {
	if len(c.config.Topics) == 0 {
		return errors.New("no topics configured for consumer")
	}
	if c.reader == nil {
		// Initialize consumer with specified topics
		if err := c.initConsumer(topics); err != nil {
			return fmt.Errorf("consumer init failed: %w", err)
		}
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				msg, err := c.reader.ReadMessage(c.ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Printf("Error reading message: %v", err)
					continue
				}

				if err := handler(msg.Value); err != nil {
					log.Printf("Error processing message: %v", err)
					continue
				}

				if err := c.reader.CommitMessages(c.ctx, msg); err != nil {
					log.Printf("Error committing message: %v", err)
				}
			}
		}
	}()

	return nil
}
