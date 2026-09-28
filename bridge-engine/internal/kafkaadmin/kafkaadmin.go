package kafkaadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates topic if it doesn't exist; an existing topic is left
// exactly as it is. replicationFactor is capped at the number of brokers so
// a single-broker dev setup still works with a production default.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int) error {
	return ensureTopic(ctx, brokers, topic, partitions, replicationFactor, nil)
}

// EnsureCompactedTopic is like EnsureTopic but sets cleanup.policy=compact,
// for internal topics that hold latest-value-per-key state (cluster
// heartbeats, placements) rather than an event log.
func EnsureCompactedTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int) error {
	return ensureTopic(ctx, brokers, topic, partitions, replicationFactor, []kafka.ConfigEntry{
		{ConfigName: "cleanup.policy", ConfigValue: "compact"},
	})
}

func ensureTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int, configEntries []kafka.ConfigEntry) error {
	if partitions < 1 {
		partitions = 1
	}

	conn, err := DialAny(ctx, brokers)
	if err != nil {
		return err
	}
	defer conn.Close()

	clusterBrokers, err := conn.Brokers()
	if err != nil {
		return fmt.Errorf("listing brokers: %w", err)
	}
	rf := replicationFactor
	if rf < 1 {
		rf = 1
	}
	if rf > len(clusterBrokers) {
		rf = len(clusterBrokers)
	}
	minISR := 1
	if rf >= 3 {
		minISR = 2
	}
	configEntries = append(configEntries, kafka.ConfigEntry{ConfigName: "min.insync.replicas", ConfigValue: strconv.Itoa(minISR)})

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
		ReplicationFactor: rf,
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

// PartitionCount returns how many partitions topic has, or 0 if it doesn't
// exist yet.
func PartitionCount(ctx context.Context, brokers []string, topic string) (int, error) {
	conn, err := DialAny(ctx, brokers)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			return 0, nil
		}
		return 0, err
	}
	return len(parts), nil
}

// DialAny connects to the first reachable broker, so one broker being down
// doesn't stop ARK from reaching the cluster.
func DialAny(ctx context.Context, brokers []string) (*kafka.Conn, error) {
	var errs []error
	for _, b := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", b)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", b, err))
	}
	return nil, fmt.Errorf("no broker reachable: %w", errors.Join(errs...))
}

// WaitReady blocks until a broker answers a metadata request, backing off up
// to max between tries, so ARK started before Kafka waits for it instead of
// exiting. It returns early only when ctx ends.
func WaitReady(ctx context.Context, brokers []string, max time.Duration, log *slog.Logger) error {
	wait := time.Second
	for {
		conn, err := DialAny(ctx, brokers)
		if err == nil {
			_, err = conn.Brokers()
			_ = conn.Close()
			if err == nil {
				return nil
			}
		}
		log.Warn("waiting for Kafka", "brokers", brokers, "error", err, "retry_in", wait.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, max)
	}
}

// DialLeaderAny connects to the leader of topic/partition, looking it up
// through the first reachable broker.
func DialLeaderAny(ctx context.Context, brokers []string, topic string, partition int) (*kafka.Conn, error) {
	var errs []error
	for _, b := range brokers {
		conn, err := kafka.DialLeader(ctx, "tcp", b, topic, partition)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", b, err))
	}
	return nil, fmt.Errorf("no broker could reach the leader of %s/%d: %w", topic, partition, errors.Join(errs...))
}

// ConsumerGroups lists the consumer group IDs the cluster knows about.
func ConsumerGroups(ctx context.Context, brokers []string) ([]string, error) {
	c := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := c.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", resp.Error)
	}
	groups := make([]string, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groups = append(groups, g.GroupID)
	}
	sort.Strings(groups)
	return groups, nil
}
