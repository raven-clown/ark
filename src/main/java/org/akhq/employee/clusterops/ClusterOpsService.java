package org.akhq.employee.clusterops;

import jakarta.inject.Singleton;
import org.akhq.modules.KafkaModule;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class ClusterOpsService {
    private final KafkaModule kafkaModule;

    public ClusterOpsService(KafkaModule kafkaModule) {
        this.kafkaModule = kafkaModule;
    }

    public List<TransactionSummary> listTransactions(String clusterId) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        List<TransactionListing> listings = admin.listTransactions().all().get().stream().toList();
        return listings.stream()
            .map(t -> new TransactionSummary(t.transactionalId(), t.producerId(), t.state().toString()))
            .collect(Collectors.toList());
    }

    public TransactionDetail describeTransaction(String clusterId, String transactionalId) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        TransactionDescription description = admin.describeTransactions(List.of(transactionalId))
            .description(transactionalId).get();

        List<String> partitions = description.topicPartitions().stream()
            .map(tp -> tp.topic() + "-" + tp.partition())
            .collect(Collectors.toList());

        return new TransactionDetail(
            transactionalId, description.coordinatorId(), description.state().toString(),
            description.producerId(), description.producerEpoch(),
            description.transactionStartTimeMs().isPresent() ? description.transactionStartTimeMs().getAsLong() : null,
            partitions
        );
    }

    public void abortTransaction(String clusterId, AbortTransactionRequest request) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        TopicPartition topicPartition = new TopicPartition(request.topic, request.partition);
        AbortTransactionSpec spec = new AbortTransactionSpec(
            topicPartition, request.producerId, request.producerEpoch, request.coordinatorEpoch
        );
        admin.abortTransaction(spec).all().get();
    }

    public void electPreferredLeader(String clusterId, String topic, int partition) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        Set<TopicPartition> partitions = Set.of(new TopicPartition(topic, partition));
        admin.electLeaders(ElectionType.PREFERRED, partitions).all().get();
    }

    public List<LogDirEntry> describeLogDirs(String clusterId, List<Integer> brokerIds) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        List<Integer> targetBrokerIds = brokerIds;
        if (targetBrokerIds == null || targetBrokerIds.isEmpty()) {
            targetBrokerIds = admin.describeCluster().nodes().get().stream()
                .map(org.apache.kafka.common.Node::id)
                .collect(Collectors.toList());
        }
        Map<Integer, Map<String, LogDirDescription>> all = admin.describeLogDirs(targetBrokerIds).allDescriptions().get();

        List<LogDirEntry> entries = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, LogDirDescription>> brokerEntry : all.entrySet()) {
            for (Map.Entry<String, LogDirDescription> dirEntry : brokerEntry.getValue().entrySet()) {
                LogDirDescription description = dirEntry.getValue();
                Long totalBytes = description.totalBytes().isPresent() ? description.totalBytes().getAsLong() : null;
                Long usableBytes = description.usableBytes().isPresent() ? description.usableBytes().getAsLong() : null;
                entries.add(new LogDirEntry(
                    brokerEntry.getKey(), dirEntry.getKey(), totalBytes, usableBytes, description.isCordoned()
                ));
            }
        }
        return entries;
    }

    public QuorumView describeMetadataQuorum(String clusterId) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        QuorumInfo info = admin.describeMetadataQuorum().quorumInfo().get();

        return new QuorumView(
            info.leaderId(), info.leaderEpoch(), info.highWatermark(),
            info.voters().stream().map(v -> new QuorumReplicaView(v.replicaId(), v.logEndOffset())).collect(Collectors.toList()),
            info.observers().stream().map(v -> new QuorumReplicaView(v.replicaId(), v.logEndOffset())).collect(Collectors.toList())
        );
    }
}
