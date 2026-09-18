package org.akhq.employee.repository;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;
import org.akhq.employee.domain.Employee;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface EmployeeRepository extends CrudRepository<Employee, Long> {
    Optional<Employee> findByEmployeeCode(String employeeCode);

    @Query("select * from employees order by full_name asc")
    List<Employee> findAllByOrderByFullNameAsc();

    @Query("update employees set is_admin = :admin, updated_at = now() where employee_code = :employeeCode")
    void updateAdminFlag(String employeeCode, boolean admin);

    @Query("update employees set is_super_admin = :superAdmin, updated_at = now() where employee_code = :employeeCode")
    void updateSuperAdminFlag(String employeeCode, boolean superAdmin);

    @Query("update employees set is_active = :active, updated_at = now() where employee_code = :employeeCode")
    void updateActiveFlag(String employeeCode, boolean active);

    @Query("update employees set last_login_at = now() where id = :id")
    void touchLastLogin(Long id);

    @Query("update employees set password_hash = :passwordHash, updated_at = now() where employee_code = :employeeCode")
    void updatePasswordHash(String employeeCode, String passwordHash);
}
