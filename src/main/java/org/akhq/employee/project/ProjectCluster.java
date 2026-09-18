package org.akhq.employee.project;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@MappedEntity("project_clusters")
@Data
@NoArgsConstructor
public class ProjectCluster {
    @Id
    @GeneratedValue
    private Long id;

    private Long projectId;
    private String clusterName;
    private String addedBy;

    @DateCreated
    private Instant addedAt;

    public ProjectCluster(Long projectId, String clusterName, String addedBy) {
        this.projectId = projectId;
        this.clusterName = clusterName;
        this.addedBy = addedBy;
    }
}
