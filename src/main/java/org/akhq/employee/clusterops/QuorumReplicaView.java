package org.akhq.employee.clusterops;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record QuorumReplicaView(int replicaId, long logEndOffset) {
}
