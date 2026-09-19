package org.akhq.employee.clusterops;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record LogDirEntry(int brokerId, String path, Long totalBytes, Long usableBytes, boolean cordoned) {
}
