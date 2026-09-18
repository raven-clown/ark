package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;
import org.akhq.employee.domain.Employee;

import java.time.Instant;
import java.util.List;

@Introspected
public record EmployeeView(Long id, String employeeCode, String fullName, boolean admin, boolean superAdmin,
                            boolean active, String accountType, Instant lastLoginAt, List<String> teams,
                            List<PermissionGrantView> grants) {
    public static EmployeeView from(Employee employee, List<String> teams, List<PermissionGrantView> grants) {
        return new EmployeeView(
            employee.getId(), employee.getEmployeeCode(), employee.getFullName(),
            employee.isAdmin(), employee.isSuperAdmin(), employee.isActive(),
            employee.getAccountType().name(), employee.getLastLoginAt(), teams, grants
        );
    }
}
