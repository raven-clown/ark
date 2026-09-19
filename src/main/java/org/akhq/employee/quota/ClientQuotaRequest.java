package org.akhq.employee.quota;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Introspected
public class ClientQuotaRequest {
    @NotNull
    public ClientQuotaEntityType entityType;

    @NotBlank
    public String entityName;

    public Double producerByteRate;
    public Double consumerByteRate;
    public Double requestPercentage;
}
