package org.akhq.employee.cluster;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface ClusterConnectionDefinitionRepository extends CrudRepository<ClusterConnectionDefinition, Long> {
    Optional<ClusterConnectionDefinition> findByName(String name);

    @Query("select * from cluster_connection_definitions order by name asc")
    List<ClusterConnectionDefinition> findAllByOrderByNameAsc();

    void deleteByName(String name);
}
