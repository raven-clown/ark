package org.akhq.employee.cluster;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record NamedUrl(String name, String url) {
}
