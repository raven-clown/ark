package org.akhq.employee.clusterops;

import io.micronaut.core.annotation.Introspected;

import java.util.List;

@Introspected
public record TransactionDetail(String transactionalId, int coordinatorId, String state, long producerId,
                                 int producerEpoch, Long transactionStartTimeMs, List<String> topicPartitions) {
}
