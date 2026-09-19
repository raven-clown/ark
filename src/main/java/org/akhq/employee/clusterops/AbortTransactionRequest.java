package org.akhq.employee.clusterops;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Introspected
public class AbortTransactionRequest {
    @NotBlank
    public String topic;

    @NotNull
    public Integer partition;

    @NotNull
    public Long producerId;

    @NotNull
    public Short producerEpoch;

    @NotNull
    public Integer coordinatorEpoch;
}
