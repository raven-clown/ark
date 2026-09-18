package org.akhq.employee.project;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;

@Introspected
public class AddClusterRequest {
    @NotBlank
    public String clusterName;
}
