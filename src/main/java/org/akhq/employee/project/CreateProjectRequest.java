package org.akhq.employee.project;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;

@Introspected
public class CreateProjectRequest {
    @NotBlank
    public String name;

    @NotBlank
    public String slug;

    public String description;
}
