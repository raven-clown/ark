package org.akhq.employee.cluster;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;

import java.util.List;

@Introspected
public class ClusterConnectionRequest {
    @NotBlank
    public String name;

    @NotBlank
    public String bootstrapServers;

    public String schemaRegistryUrl;
    public String schemaRegistryType = "CONFLUENT";
    public List<NamedUrl> connects = List.of();
    public List<NamedUrl> ksqldbs = List.of();
}
