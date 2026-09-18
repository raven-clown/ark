package org.akhq.employee.repository;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;
import org.akhq.employee.domain.PermissionGrant;

import java.util.List;

@JdbcRepository(dialect = Dialect.H2)
public interface PermissionGrantRepository extends CrudRepository<PermissionGrant, Long> {
    List<PermissionGrant> findByEmployeeId(Long employeeId);

    void deleteByIdAndEmployeeId(Long id, Long employeeId);

    void deleteByGrantedBy(String grantedBy);

    void deleteByEmployeeIdAndGrantedByStartingWith(Long employeeId, String grantedByPrefix);

    void deleteByGrantedByStartingWith(String grantedByPrefix);
}
