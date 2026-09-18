package org.akhq.employee.project;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@MappedEntity("projects")
@Data
@NoArgsConstructor
public class Project {
    @Id
    @GeneratedValue
    private Long id;

    private String name;
    private String slug;
    private String description;
    private String createdBy;

    @DateCreated
    private Instant createdAt;

    @DateUpdated
    private Instant updatedAt;

    public Project(String name, String slug, String description, String createdBy) {
        this.name = name;
        this.slug = slug;
        this.description = description;
        this.createdBy = createdBy;
    }
}
