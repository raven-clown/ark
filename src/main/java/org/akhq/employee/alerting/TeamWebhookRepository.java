package org.akhq.employee.alerting;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;

@JdbcRepository(dialect = Dialect.H2)
public interface TeamWebhookRepository extends CrudRepository<TeamWebhook, Long> {
    List<TeamWebhook> findByTeamId(Long teamId);

    List<TeamWebhook> findByTeamIdInAndEnabled(List<Long> teamIds, boolean enabled);

    void deleteByIdAndTeamId(Long id, Long teamId);
}
