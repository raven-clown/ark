package org.akhq.employee.quota;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record ClientQuotaEntry(String entityType, String entityName, Double producerByteRate,
                                Double consumerByteRate, Double requestPercentage) {
}
