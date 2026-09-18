package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Introspected
public class PasswordResetRequest {
    @NotBlank
    @Size(min = 8)
    public String password;
}
