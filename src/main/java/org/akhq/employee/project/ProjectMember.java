package org.akhq.employee.project;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.TypeDef;
import io.micronaut.data.model.DataType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@MappedEntity("project_members")
@Data
@NoArgsConstructor
public class ProjectMember {
    @Id
    @GeneratedValue
    private Long id;

    private Long projectId;
    private Long employeeId;

    @TypeDef(type = DataType.STRING)
    private ProjectRole projectRole;

    private String addedBy;

    @DateCreated
    private Instant addedAt;

    public ProjectMember(Long projectId, Long employeeId, ProjectRole projectRole, String addedBy) {
        this.projectId = projectId;
        this.employeeId = employeeId;
        this.projectRole = projectRole;
        this.addedBy = addedBy;
    }
}
