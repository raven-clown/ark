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

func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int) error {
	return ensureTopic(ctx, brokers, topic, partitions, replicationFactor, nil)
}

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

// PartitionLag is where a consumer group stands on one partition.
type PartitionLag struct {
	Partition int   `json:"partition"`
	Committed int64 `json:"committed"`
	End       int64 `json:"end"`
	Lag       int64 `json:"lag"`
}

func GroupLag(ctx context.Context, brokers []string, group, topic string) ([]PartitionLag, error) {
	n, err := PartitionCount(ctx, brokers, topic)
	if err != nil || n == 0 {
		return nil, err
	}
	parts := make([]int, n)
	reqs := make([]kafka.OffsetRequest, 0, 2*n)
	for i := range parts {
		parts[i] = i
		reqs = append(reqs, kafka.FirstOffsetOf(i), kafka.LastOffsetOf(i))
	}
	c := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	ends, err := c.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: reqs}})
	if err != nil {
		return nil, fmt.Errorf("reading the offsets of %s: %w", topic, err)
	}
	committed, err := c.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: parts}})
	if err != nil {
		return nil, fmt.Errorf("reading %s's committed offsets: %w", group, err)
	}
	if committed.Error != nil {
		return nil, fmt.Errorf("reading %s's committed offsets: %w", group, committed.Error)
	}
	out := make([]PartitionLag, n)
	first := make([]int64, n)
	for i := range out {
		out[i].Partition, out[i].Committed = i, -1
	}
	for _, p := range ends.Topics[topic] {
		if p.Error == nil && p.Partition >= 0 && p.Partition < n {
			out[p.Partition].End, first[p.Partition] = p.LastOffset, p.FirstOffset
		}
	}
	for _, p := range committed.Topics[topic] {
		if p.Error == nil && p.Partition >= 0 && p.Partition < n {
			out[p.Partition].Committed = p.CommittedOffset
		}
	}
	for i := range out {
		from := out[i].Committed
		if from < 0 {
			from = first[i]
		}
		out[i].Lag = max(0, out[i].End-from)
	}
	return out, nil
}

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
