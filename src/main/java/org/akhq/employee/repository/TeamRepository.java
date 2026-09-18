package org.akhq.employee.repository;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;
import org.akhq.employee.domain.Team;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface TeamRepository extends CrudRepository<Team, Long> {
    Optional<Team> findByName(String name);

    @Query("select * from teams order by name asc")
    List<Team> findAllByOrderByNameAsc();
}
