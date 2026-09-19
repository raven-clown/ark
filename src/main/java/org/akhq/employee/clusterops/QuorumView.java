package org.akhq.employee.clusterops;

import io.micronaut.core.annotation.Introspected;

import java.util.List;

@Introspected
public record QuorumView(int leaderId, long leaderEpoch, long highWatermark,
                          List<QuorumReplicaView> voters, List<QuorumReplicaView> observers) {
}
