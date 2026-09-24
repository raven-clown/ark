package kafkaadmin

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	return ensureTopic(ctx, brokers, topic, partitions, nil)
}

// EnsureCompactedTopic is like EnsureTopic but sets cleanup.policy=compact,
// for internal topics that hold latest-value-per-key state (cluster
// heartbeats, placements) rather than an event log.
func EnsureCompactedTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	return ensureTopic(ctx, brokers, topic, partitions, []kafka.ConfigEntry{
		{ConfigName: "cleanup.policy", ConfigValue: "compact"},
	})
}

func ensureTopic(ctx context.Context, brokers []string, topic string, partitions int, configEntries []kafka.ConfigEntry) error {
	if partitions < 1 {
		partitions = 1
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dialing %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("finding controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dialing controller %s: %w", controllerAddr, err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
		ConfigEntries:     configEntries,
	})
	if err != nil {
		return fmt.Errorf("creating topic %s: %w", topic, err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		parts, err := conn.ReadPartitions(topic)
		if err == nil && len(parts) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("topic %s did not become visible within timeout", topic)
}
