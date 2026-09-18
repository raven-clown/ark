package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotNull;
import org.akhq.configs.security.Role;

@Introspected
public class PermissionGrantRequest {
    @NotNull
    public Role.Resource resource;

    @NotNull
    public Role.Action action;

    public String clusterPattern = ".*";
    public String topicPattern = ".*";
}
