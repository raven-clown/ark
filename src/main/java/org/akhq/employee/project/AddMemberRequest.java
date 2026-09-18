package org.akhq.employee.project;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Introspected
public class AddMemberRequest {
    @NotBlank
    public String employeeCode;

    @NotNull
    public ProjectRole projectRole;
}
