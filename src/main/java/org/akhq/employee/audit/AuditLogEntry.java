package org.akhq.employee.audit;

import io.micronaut.core.annotation.Introspected;

import java.time.Instant;
import java.util.Map;

@Introspected
public record AuditLogEntry(Instant timestamp, int partition, long offset, String type,
                             String userName, String actionType, Map<String, Object> details) {
}
