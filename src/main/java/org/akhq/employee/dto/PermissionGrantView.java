package org.akhq.employee.dto;

import io.micronaut.core.annotation.Introspected;
import org.akhq.configs.security.Role;
import org.akhq.employee.domain.PermissionGrant;
import org.akhq.employee.domain.TeamPermissionGrant;

@Introspected
public record PermissionGrantView(Long id, String source, Role.Resource resource, Role.Action action,
                                   String clusterPattern, String topicPattern, String grantedBy) {
    public static PermissionGrantView fromEmployeeGrant(PermissionGrant grant) {
        return new PermissionGrantView(
            grant.getId(), "INDIVIDUAL", grant.getResource(), grant.getAction(),
            grant.getClusterPattern(), grant.getTopicPattern(), grant.getGrantedBy()
        );
    }

    public static PermissionGrantView fromTeamGrant(TeamPermissionGrant grant, String teamName) {
        return new PermissionGrantView(
            grant.getId(), "TEAM:" + teamName, grant.getResource(), grant.getAction(),
            grant.getClusterPattern(), grant.getTopicPattern(), grant.getGrantedBy()
        );
    }
}
