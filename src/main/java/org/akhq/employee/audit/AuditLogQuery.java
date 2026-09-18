package org.akhq.employee.audit;

import java.time.Instant;

public record AuditLogQuery(String employeeCode, String team, String actionType, Instant from, Instant to) {
}
