package org.akhq.employee.directory;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.Optional;

@Singleton
@Requires(property = "akhq.employee-directory.mock-enabled", value = "true", defaultValue = "true")
public class MockEmployeeDirectoryClient implements EmployeeDirectoryClient {
    @Override
    public Optional<EmployeeDirectoryRecord> lookup(String employeeCode) {
        if (employeeCode == null || employeeCode.isBlank()) {
            return Optional.empty();
        }
        String name = "พนักงาน " + employeeCode;
        return Optional.of(new EmployeeDirectoryRecord(employeeCode.trim(), name));
    }
}
