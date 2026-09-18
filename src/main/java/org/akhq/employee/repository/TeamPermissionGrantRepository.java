package org.akhq.employee.repository;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;
import org.akhq.employee.domain.TeamPermissionGrant;

import java.util.Collection;
import java.util.List;

@JdbcRepository(dialect = Dialect.H2)
public interface TeamPermissionGrantRepository extends CrudRepository<TeamPermissionGrant, Long> {
    List<TeamPermissionGrant> findByTeamId(Long teamId);

    List<TeamPermissionGrant> findByTeamIdIn(Collection<Long> teamIds);

    void deleteByIdAndTeamId(Long id, Long teamId);
}
