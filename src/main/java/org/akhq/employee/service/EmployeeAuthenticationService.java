package org.akhq.employee.service;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.configs.security.Role;
import org.akhq.employee.config.EmployeeDirectoryProperties;
import org.akhq.employee.directory.EmployeeDirectoryClient;
import org.akhq.employee.directory.EmployeeDirectoryRecord;
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

        // Bootstrap admin/super admin status only applies the first time this account is created.
        // Re-asserting it on every login would silently undo a deliberate later demotion by a real
        // super admin - bootstrap-admin-codes is a one-time seed, not a standing override.
        // Refused entirely while the mock directory is active: mock-enabled means any string is a
        // valid "employee", so bootstrap codes would let anyone become super admin with one request.
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

        // A freshly bootstrapped super admin starts with zero grants, which would lock them out of
        // every cluster-scoped screen - including the admin dashboard meant to grant permissions in
        // the first place. Seed full access once, on the login that creates the account.
        if (isNewEmployee && saved.isSuperAdmin()) {
            seedFullAccess(saved);
        }

        employeeRepository.touchLastLogin(saved.getId());
        saved.setLastLoginAt(Instant.now());
        return Optional.of(saved);
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
