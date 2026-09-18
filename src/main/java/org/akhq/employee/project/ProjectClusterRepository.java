package org.akhq.employee.project;

import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;
import java.util.Optional;

@JdbcRepository(dialect = Dialect.H2)
public interface ProjectClusterRepository extends CrudRepository<ProjectCluster, Long> {
    List<ProjectCluster> findByProjectId(Long projectId);

    Optional<ProjectCluster> findByProjectIdAndClusterName(Long projectId, String clusterName);

    void deleteByProjectIdAndClusterName(Long projectId, String clusterName);
}
