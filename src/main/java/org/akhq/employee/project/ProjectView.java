package org.akhq.employee.project;

import io.micronaut.core.annotation.Introspected;

import java.time.Instant;
import java.util.List;

@Introspected
public record ProjectView(Long id, String name, String slug, String description, String createdBy,
                           Instant createdAt, List<ProjectMemberView> members, List<String> clusters,
                           ProjectRole callerRole) {
}
