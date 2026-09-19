package org.akhq.employee.scram;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Introspected
public class ScramCredentialRequest {
    @NotBlank
    public String username;

    @NotNull
    public ScramMechanismChoice mechanism;

    @NotBlank
    public String password;

    public int iterations = 4096;
}
