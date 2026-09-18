package org.akhq.employee.service;

import jakarta.inject.Singleton;
import org.akhq.employee.domain.AccountType;
import org.akhq.employee.domain.Employee;
import org.akhq.employee.repository.EmployeeRepository;
import org.mindrot.jbcrypt.BCrypt;

import java.util.NoSuchElementException;
import java.util.Optional;

@Singleton
public class ManualAccountService {
    private final EmployeeRepository employeeRepository;

    public ManualAccountService(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    public Employee createAccount(String employeeCode, String fullName, String rawPassword, boolean admin) {
        if (employeeRepository.findByEmployeeCode(employeeCode).isPresent()) {
            throw new IllegalArgumentException("Employee code already exists: " + employeeCode);
        }
        Employee employee = new Employee(employeeCode, fullName, AccountType.MANUAL, hash(rawPassword));
        employee.setAdmin(admin);
        return employeeRepository.save(employee);
    }

    public void resetPassword(String employeeCode, String rawPassword) {
        requireManualAccount(employeeCode);
        employeeRepository.updatePasswordHash(employeeCode, hash(rawPassword));
    }

    public Optional<Employee> authenticate(String employeeCode, String rawPassword) {
        return employeeRepository.findByEmployeeCode(employeeCode)
            .filter(e -> e.getAccountType() == AccountType.MANUAL)
            .filter(Employee::isActive)
            .filter(e -> e.getPasswordHash() != null && BCrypt.checkpw(rawPassword, e.getPasswordHash()));
    }

    private void requireManualAccount(String employeeCode) {
        Employee employee = employeeRepository.findByEmployeeCode(employeeCode)
            .orElseThrow(() -> new NoSuchElementException("Employee not found: " + employeeCode));
        if (employee.getAccountType() != AccountType.MANUAL) {
            throw new IllegalArgumentException("Employee " + employeeCode + " is not a manual account");
        }
    }

    private String hash(String rawPassword) {
        return BCrypt.hashpw(rawPassword, BCrypt.gensalt());
    }
}
