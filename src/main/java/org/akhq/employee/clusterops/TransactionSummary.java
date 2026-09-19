package org.akhq.employee.clusterops;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record TransactionSummary(String transactionalId, long producerId, String state) {
}
