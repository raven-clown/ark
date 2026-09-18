package org.akhq.employee.project;

import io.micronaut.core.annotation.Introspected;

@Introspected
public record ProjectMemberView(String employeeCode, String fullName, ProjectRole projectRole) {
}
