package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;

@Introspected
public class AssignTeamRequest {
    @NotBlank
    public String teamName;
}
