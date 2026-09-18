package org.akhq.employee.cluster;

import io.micronaut.core.annotation.Introspected;

import java.time.Instant;
import java.util.List;

@Introspected
public record ClusterConnectionView(Long id, String name, String bootstrapServers, String schemaRegistryUrl,
                                     String schemaRegistryType, List<NamedUrl> connects, List<NamedUrl> ksqldbs,
                                     String createdBy, Instant createdAt, String yamlSnippet) {
}
