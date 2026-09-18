package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;

import java.util.List;

@Introspected
public record RbacExport(List<EmployeeExport> employees, List<TeamExport> teams) {

    @Introspected
    public record EmployeeExport(String employeeCode, String fullName, boolean admin, boolean superAdmin,
                                  boolean active, List<String> teams, List<PermissionGrantView> grants) {
    }

    @Introspected
    public record TeamExport(String name, String description, List<PermissionGrantView> grants) {
    }
}
