package org.akhq.employee.project;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface ProjectRepository extends CrudRepository<Project, Long> {
    Optional<Project> findBySlug(String slug);

    @Query("select * from projects order by name asc")
    List<Project> findAllByOrderByNameAsc();
}
