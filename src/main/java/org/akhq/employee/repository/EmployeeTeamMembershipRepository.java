package org.akhq.employee.repository;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;
import org.akhq.employee.domain.EmployeeTeamMembership;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface EmployeeTeamMembershipRepository extends CrudRepository<EmployeeTeamMembership, Long> {
    List<EmployeeTeamMembership> findByEmployeeId(Long employeeId);

    List<EmployeeTeamMembership> findByTeamId(Long teamId);

    Optional<EmployeeTeamMembership> findByEmployeeIdAndTeamId(Long employeeId, Long teamId);

    void deleteByEmployeeIdAndTeamId(Long employeeId, Long teamId);
}
