package org.akhq.employee.directory;

import java.util.Optional;

public interface EmployeeDirectoryClient {
    Optional<EmployeeDirectoryRecord> lookup(String employeeCode);
}
