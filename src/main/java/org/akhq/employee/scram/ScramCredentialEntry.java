package org.akhq.employee.scram;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record ScramCredentialEntry(String username, String mechanism, int iterations) {
}
