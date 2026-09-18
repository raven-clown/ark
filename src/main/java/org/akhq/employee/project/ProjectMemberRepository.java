package org.akhq.employee.project;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface ProjectMemberRepository extends CrudRepository<ProjectMember, Long> {
    List<ProjectMember> findByProjectId(Long projectId);

    List<ProjectMember> findByEmployeeId(Long employeeId);

    Optional<ProjectMember> findByProjectIdAndEmployeeId(Long projectId, Long employeeId);

    void deleteByProjectIdAndEmployeeId(Long projectId, Long employeeId);
}
