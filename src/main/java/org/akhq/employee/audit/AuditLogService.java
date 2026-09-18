package org.akhq.employee.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.configs.Audit;
import org.akhq.employee.domain.Team;
import org.akhq.employee.repository.EmployeeRepository;
import org.akhq.employee.repository.EmployeeTeamMembershipRepository;
import org.akhq.employee.repository.TeamRepository;
import org.akhq.modules.KafkaModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads AKHQ's own akhq-audit topic back out for the admin dashboard. Every scan is bounded on
 * both records inspected and wall-clock polls, regardless of filters, so a broad or empty-result
 * query cannot turn into an unbounded read against a live topic.
 */
@Slf4j
@Singleton
public class AuditLogService {
    private static final int MAX_RECORDS_SCANNED = 20_000;
    private static final int MAX_RESULTS = 500;
    private static final int MAX_POLLS = 40;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaModule kafkaModule;
    private final Audit auditConfig;
    private final EmployeeRepository employeeRepository;
    private final EmployeeTeamMembershipRepository membershipRepository;
    private final TeamRepository teamRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    public AuditLogService(KafkaModule kafkaModule, Audit auditConfig, EmployeeRepository employeeRepository,
                            EmployeeTeamMembershipRepository membershipRepository, TeamRepository teamRepository) {
        this.kafkaModule = kafkaModule;
        this.auditConfig = auditConfig;
        this.employeeRepository = employeeRepository;
        this.membershipRepository = membershipRepository;
        this.teamRepository = teamRepository;
    }

    public List<AuditLogEntry> search(AuditLogQuery query) {
        if (!Boolean.TRUE.equals(auditConfig.getEnabled())) {
            return List.of();
        }

        Set<String> teamMemberCodes = query.team() == null ? null : resolveTeamMemberCodes(query.team());
        if (teamMemberCodes != null && teamMemberCodes.isEmpty()) {
            return List.of();
        }

        String clusterId = auditConfig.getClusterId();
        String topicName = auditConfig.getTopicName();
        Instant from = query.from() != null ? query.from() : Instant.now().minus(Duration.ofDays(1));

        List<AuditLogEntry> results = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = kafkaModule.getConsumer(clusterId)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                return List.of();
            }
            List<TopicPartition> partitions = partitionInfos.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toList());
            consumer.assign(partitions);

            Map<TopicPartition, Long> searchTimestamps = new HashMap<>();
            partitions.forEach(tp -> searchTimestamps.put(tp, from.toEpochMilli()));
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(searchTimestamps);

            for (TopicPartition tp : partitions) {
                OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(tp);
                if (offsetAndTimestamp != null) {
                    consumer.seek(tp, offsetAndTimestamp.offset());
                } else {
                    consumer.seekToEnd(List.of(tp));
                }
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            int scanned = 0;
            int polls = 0;
            while (scanned < MAX_RECORDS_SCANNED && polls < MAX_POLLS && results.size() < MAX_RESULTS) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                polls++;

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    scanned++;
                    AuditLogEntry entry = parse(record);
                    if (entry != null && matches(entry, query, teamMemberCodes)) {
                        results.add(entry);
                        if (results.size() >= MAX_RESULTS) {
                            break;
                        }
                    }
                    if (scanned >= MAX_RECORDS_SCANNED) {
                        break;
                    }
                }

                boolean caughtUp = partitions.stream()
                    .allMatch(tp -> consumer.position(tp) >= endOffsets.getOrDefault(tp, 0L));
                if (caughtUp) {
                    break;
                }
            }
        } catch (Exception e) {
            log.warn("Failed reading audit log from cluster {} topic {}", clusterId, topicName, e);
            return List.of();
        }

        results.sort(Comparator.comparing(AuditLogEntry::timestamp).reversed());
        return results;
    }

    private boolean matches(AuditLogEntry entry, AuditLogQuery query, Set<String> teamMemberCodes) {
        if (query.to() != null && entry.timestamp().isAfter(query.to())) {
            return false;
        }
        if (query.employeeCode() != null && !query.employeeCode().equalsIgnoreCase(entry.userName())) {
            return false;
        }
        if (teamMemberCodes != null && !teamMemberCodes.contains(entry.userName())) {
            return false;
        }
        if (query.actionType() != null && !query.actionType().equalsIgnoreCase(entry.actionType())) {
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private AuditLogEntry parse(ConsumerRecord<byte[], byte[]> record) {
        try {
            Map<String, Object> raw = mapper.readValue(record.value(), Map.class);
            String type = String.valueOf(raw.remove("type"));
            String userName = String.valueOf(raw.remove("userName"));
            String actionType = String.valueOf(raw.remove("actionType"));
            return new AuditLogEntry(
                Instant.ofEpochMilli(record.timestamp()), record.partition(), record.offset(),
                type, userName, actionType, raw
            );
        } catch (Exception e) {
            log.warn("Could not parse audit event at partition {} offset {}", record.partition(), record.offset(), e);
            return null;
        }
    }

    private Set<String> resolveTeamMemberCodes(String teamName) {
        return teamRepository.findByName(teamName)
            .map(Team::getId)
            .map(teamId -> membershipRepository.findByTeamId(teamId).stream()
                .map(m -> employeeRepository.findById(m.getEmployeeId()))
                .filter(Optional::isPresent)
                .map(o -> o.get().getEmployeeCode())
                .collect(Collectors.toSet()))
            .orElse(Set.of());
    }
}
