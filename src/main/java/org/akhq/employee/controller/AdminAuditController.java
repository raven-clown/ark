package org.akhq.employee.controller;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import org.akhq.employee.audit.AuditLogEntry;
import org.akhq.employee.audit.AuditLogQuery;
import org.akhq.employee.audit.AuditLogService;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Secured("ROLE_ADMIN")
@Controller("/api/admin/audit-log")
public class AdminAuditController {
    @Inject
    private AuditLogService auditLogService;

    @Get
    @Operation(tags = {"admin"}, summary = "Search the akhq-audit topic, filtered by employee, team, action type and time range")
    public List<AuditLogEntry> search(
        Optional<String> employeeCode,
        Optional<String> team,
        Optional<String> actionType,
        Optional<Long> fromEpochMilli,
        Optional<Long> toEpochMilli
    ) {
        AuditLogQuery query = new AuditLogQuery(
            employeeCode.orElse(null),
            team.orElse(null),
            actionType.orElse(null),
            fromEpochMilli.map(Instant::ofEpochMilli).orElse(null),
            toEpochMilli.map(Instant::ofEpochMilli).orElse(null)
        );
        return auditLogService.search(query);
    }
}
