package org.akhq.employee.cluster;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * A cluster connection an admin has defined through the dashboard. This is a definition store
 * only - it does not (yet) feed into KafkaModule's client factory, so adding a row here does not
 * make the cluster reachable without also pasting the generated YAML into application.yml and
 * restarting. See docs/enterprise/architecture-blueprint.md, "Dynamic cluster management".
 */
@MappedEntity("cluster_connection_definitions")
@Data
@NoArgsConstructor
public class ClusterConnectionDefinition {
    @Id
    @GeneratedValue
    private Long id;

    private String name;
    private String bootstrapServers;
    private String schemaRegistryUrl;
    private String schemaRegistryType = "CONFLUENT";

    /** JSON array of {"name": "...", "url": "..."} */
    private String connectsJson = "[]";

    /** JSON array of {"name": "...", "url": "..."} */
    private String ksqldbsJson = "[]";

    private String createdBy;

    @DateCreated
    private Instant createdAt;

    @DateUpdated
    private Instant updatedAt;
}
