package org.akhq.employee.alerting;

import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.clients.connect.KafkaConnectApiClient;
import org.akhq.clients.connect.dto.ConnectorExpanded;
import org.akhq.clients.connect.dto.ConnectorStatus;
import org.akhq.configs.Connect;
import org.akhq.configs.Connection;
import org.akhq.employee.domain.Team;
import org.akhq.employee.domain.TeamPermissionGrant;
import org.akhq.employee.repository.TeamPermissionGrantRepository;
import org.akhq.employee.repository.TeamRepository;
import org.akhq.modules.KafkaModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Scans every configured cluster for consumer lag and FAILED connectors, notifying each team's
 * webhook when they own the cluster (a TeamPermissionGrant matching it exists). Deliberately goes
 * straight through KafkaModule's plain singleton AdminClient/Connect clients rather than the
 * repository layer, since AbstractKafkaWrapper is @RequestScope and unusable from a scheduled job.
 */
@Slf4j
@Singleton
public class AlertingScheduler {
    private static final Duration ALERT_COOLDOWN = Duration.ofMinutes(30);

    private final AlertingProperties properties;
    private final List<Connection> connections;
    private final KafkaModule kafkaModule;
    private final TeamRepository teamRepository;
    private final TeamPermissionGrantRepository teamPermissionGrantRepository;
    private final TeamWebhookRepository teamWebhookRepository;
    private final WebhookSender webhookSender;

    private final Map<String, Instant> lastAlertedAt = new ConcurrentHashMap<>();

    public AlertingScheduler(AlertingProperties properties, List<Connection> connections, KafkaModule kafkaModule,
                              TeamRepository teamRepository, TeamPermissionGrantRepository teamPermissionGrantRepository,
                              TeamWebhookRepository teamWebhookRepository, WebhookSender webhookSender) {
        this.properties = properties;
        this.connections = connections;
        this.kafkaModule = kafkaModule;
        this.teamRepository = teamRepository;
        this.teamPermissionGrantRepository = teamPermissionGrantRepository;
        this.teamWebhookRepository = teamWebhookRepository;
        this.webhookSender = webhookSender;
    }

    @Scheduled(fixedDelay = "${akhq.alerting.check-interval:5m}")
    void checkAll() {
        if (!properties.isEnabled()) {
            return;
        }
        for (Connection connection : connections) {
            String clusterId = connection.getName();
            try {
                checkConsumerLag(clusterId);
            } catch (Exception e) {
                log.warn("Alerting lag check failed for cluster {}", clusterId, e);
            }
            try {
                checkConnectors(connection, clusterId);
            } catch (Exception e) {
                log.warn("Alerting connector check failed for cluster {}", clusterId, e);
            }
        }
    }

    private void checkConsumerLag(String clusterId) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().all().get();

        for (ConsumerGroupListing group : groups) {
            String groupId = group.groupId();
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) {
                continue;
            }

            Map<TopicPartition, OffsetSpec> latestSpecs = committed.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            var endOffsets = admin.listOffsets(latestSpecs).all().get();

            long totalLag = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                var end = endOffsets.get(entry.getKey());
                if (end != null) {
                    totalLag += Math.max(0, end.offset() - entry.getValue().offset());
                }
            }

            if (totalLag > properties.getLagThreshold()) {
                fireAlert(clusterId, "lag:" + groupId,
                    "Consumer group " + groupId + " on cluster " + clusterId + " has lag " + totalLag);
            }
        }
    }

    private void checkConnectors(Connection connection, String clusterId) {
        if (connection.getConnect() == null) {
            return;
        }
        Map<String, KafkaConnectApiClient> clients;
        try {
            clients = kafkaModule.getConnectRestClient(clusterId);
        } catch (Exception e) {
            log.warn("Could not reach connect clients for cluster {}", clusterId, e);
            return;
        }

        for (Connect connect : connection.getConnect()) {
            KafkaConnectApiClient client = clients.get(connect.getName());
            if (client == null) {
                continue;
            }
            try {
                Map<String, ConnectorExpanded> connectors = client.getConnectorsExpanded();
                for (Map.Entry<String, ConnectorExpanded> entry : connectors.entrySet()) {
                    ConnectorStatus status = entry.getValue().getStatus();
                    if (status == null || status.getConnector() == null) {
                        continue;
                    }
                    if ("FAILED".equals(status.getConnector().getState())) {
                        fireAlert(clusterId, "connector:" + connect.getName() + ":" + entry.getKey(),
                            "Connector " + entry.getKey() + " on " + connect.getName()
                                + " (cluster " + clusterId + ") is FAILED");
                    }
                }
            } catch (Exception e) {
                log.warn("Could not check connectors on {} for cluster {}", connect.getName(), clusterId, e);
            }
        }
    }

    private void fireAlert(String clusterId, String dedupeKey, String message) {
        Instant last = lastAlertedAt.get(dedupeKey);
        if (last != null && last.plus(ALERT_COOLDOWN).isAfter(Instant.now())) {
            return;
        }
        lastAlertedAt.put(dedupeKey, Instant.now());
        notifyTeamsForCluster(clusterId, message);
    }

    private void notifyTeamsForCluster(String clusterId, String message) {
        for (Team team : teamRepository.findAllByOrderByNameAsc()) {
            boolean teamOwnsCluster = teamPermissionGrantRepository.findByTeamId(team.getId()).stream()
                .anyMatch(g -> matchesCluster(g, clusterId));
            if (!teamOwnsCluster) {
                continue;
            }
            List<TeamWebhook> webhooks = teamWebhookRepository.findByTeamIdInAndEnabled(List.of(team.getId()), true);
            for (TeamWebhook webhook : webhooks) {
                webhookSender.send(webhook, message);
            }
        }
    }

    private boolean matchesCluster(TeamPermissionGrant grant, String clusterId) {
        try {
            return Pattern.matches(grant.getClusterPattern(), clusterId);
        } catch (PatternSyntaxException e) {
            return false;
        }
    }
}
