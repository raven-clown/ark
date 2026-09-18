package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Introspected
public class ManualAccountRequest {
    @NotBlank
    public String employeeCode;

    @NotBlank
    public String fullName;

    @NotBlank
    @Size(min = 8)
    public String password;

    public boolean admin;
}
