package org.akhq.employee.domain;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.TypeDef;
import io.micronaut.data.model.DataType;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.akhq.configs.security.Role;

import java.time.Instant;

@MappedEntity("permission_grants")
@Data
@NoArgsConstructor
public class PermissionGrant {
    @Id
    @GeneratedValue
    private Long id;

    private Long employeeId;

    @TypeDef(type = DataType.STRING)
    private Role.Resource resource;

    @TypeDef(type = DataType.STRING)
    private Role.Action action;

    private String clusterPattern = ".*";
    private String topicPattern = ".*";
    private String grantedBy;

    @DateCreated
    private Instant createdAt;

    public PermissionGrant(Long employeeId, Role.Resource resource, Role.Action action,
                            String clusterPattern, String topicPattern, String grantedBy) {
        this.employeeId = employeeId;
        this.resource = resource;
        this.action = action;
        this.clusterPattern = clusterPattern == null || clusterPattern.isBlank() ? ".*" : clusterPattern;
        this.topicPattern = topicPattern == null || topicPattern.isBlank() ? ".*" : topicPattern;
        this.grantedBy = grantedBy;
    }
}
