package org.akhq.employee.service;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.configs.security.Role;
import org.akhq.employee.config.EmployeeDirectoryProperties;
import org.akhq.employee.directory.EmployeeDirectoryClient;
import org.akhq.employee.directory.EmployeeDirectoryRecord;
import org.akhq.employee.domain.AccountType;
import org.akhq.employee.domain.Employee;
import org.akhq.employee.domain.PermissionGrant;
import org.akhq.employee.repository.EmployeeRepository;
import org.akhq.employee.repository.PermissionGrantRepository;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Singleton
public class EmployeeAuthenticationService {
    private final EmployeeDirectoryClient directoryClient;
    private final EmployeeRepository employeeRepository;
    private final PermissionGrantRepository permissionGrantRepository;
    private final EmployeeDirectoryProperties properties;

    public EmployeeAuthenticationService(EmployeeDirectoryClient directoryClient,
                                          EmployeeRepository employeeRepository,
                                          PermissionGrantRepository permissionGrantRepository,
                                          EmployeeDirectoryProperties properties) {
        this.directoryClient = directoryClient;
        this.employeeRepository = employeeRepository;
        this.permissionGrantRepository = permissionGrantRepository;
        this.properties = properties;
    }

    public Optional<Employee> authenticate(String employeeCode) {
        if (employeeCode == null || employeeCode.isBlank()) {
            return Optional.empty();
        }
        String normalizedCode = employeeCode.trim();

        Optional<EmployeeDirectoryRecord> directoryRecord = directoryClient.lookup(normalizedCode);
        if (directoryRecord.isEmpty()) {
            log.info("Employee code {} not found in central directory", normalizedCode);
            return Optional.empty();
        }

        Optional<Employee> existing = employeeRepository.findByEmployeeCode(normalizedCode);
        boolean isNewEmployee = existing.isEmpty();
        Employee employee = existing.orElseGet(() -> new Employee(normalizedCode, directoryRecord.get().fullName()));

        employee.setFullName(directoryRecord.get().fullName());

        // One time seed only, refused while mock is active to stop anyone becoming super admin
        if (isNewEmployee && properties.getBootstrapAdminCodes().contains(normalizedCode)) {
            if (properties.isMockEnabled()) {
                log.error("Refusing to grant bootstrap admin/super admin to {} while the mock employee "
                    + "directory is enabled. Set akhq.employee-directory.mock-enabled: false first.", normalizedCode);
            } else {
                employee.setAdmin(true);
                employee.setSuperAdmin(true);
            }
        }

        if (!employee.isActive()) {
            log.warn("Employee {} is deactivated locally, denying login", normalizedCode);
            return Optional.empty();
        }

        Employee saved = isNewEmployee
            ? employeeRepository.save(employee)
            : employeeRepository.update(employee);

        // Seeds full access so a new super admin is not locked out of the dashboard that grants it
        if (isNewEmployee && saved.isSuperAdmin()) {
            seedFullAccess(saved);
        }

        employeeRepository.touchLastLogin(saved.getId());
        saved.setLastLoginAt(Instant.now());
        return Optional.of(saved);
    }

    // Side effect only, does not gate OIDC/LDAP login or touch their existing permission source
    public Employee syncExternalIdentity(String username, String displayName, AccountType accountType) {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username is required");
        }
        String normalizedUsername = username.trim();
        String fullName = (displayName == null || displayName.isBlank()) ? normalizedUsername : displayName;

        Optional<Employee> existing = employeeRepository.findByEmployeeCode(normalizedUsername);
        boolean isNewEmployee = existing.isEmpty();
        Employee employee = existing.orElseGet(
            () -> new Employee(normalizedUsername, fullName, accountType, null));

        employee.setFullName(fullName);

        if (isNewEmployee && properties.getBootstrapAdminCodes().contains(normalizedUsername)) {
            employee.setAdmin(true);
            employee.setSuperAdmin(true);
        }

        Employee saved = isNewEmployee
            ? employeeRepository.save(employee)
            : employeeRepository.update(employee);

        if (isNewEmployee && saved.isSuperAdmin()) {
            seedFullAccess(saved);
        }

        employeeRepository.touchLastLogin(saved.getId());
        return saved;
    }

    private void seedFullAccess(Employee employee) {
        for (Role.Resource resource : Role.Resource.values()) {
            for (Role.Action action : Role.Action.values()) {
                permissionGrantRepository.save(
                    new PermissionGrant(employee.getId(), resource, action, ".*", ".*", "bootstrap")
                );
            }
        }
    }
}
