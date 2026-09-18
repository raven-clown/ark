package org.akhq.employee.domain;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.annotation.TypeDef;
import io.micronaut.data.model.DataType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@MappedEntity("employees")
@Data
@NoArgsConstructor
public class Employee {
    @Id
    @GeneratedValue
    private Long id;

    private String employeeCode;
    private String fullName;

    @MappedProperty("is_admin")
    private boolean admin;

    @MappedProperty("is_super_admin")
    private boolean superAdmin;

    @MappedProperty("is_active")
    private boolean active = true;

    @TypeDef(type = DataType.STRING)
    private AccountType accountType = AccountType.EMPLOYEE_DIRECTORY;

    private String passwordHash;

    @DateCreated
    private Instant createdAt;

    @DateUpdated
    private Instant updatedAt;

    private Instant lastLoginAt;

    public Employee(String employeeCode, String fullName) {
        this.employeeCode = employeeCode;
        this.fullName = fullName;
    }

    public Employee(String employeeCode, String fullName, AccountType accountType, String passwordHash) {
        this.employeeCode = employeeCode;
        this.fullName = fullName;
        this.accountType = accountType;
        this.passwordHash = passwordHash;
    }
}
