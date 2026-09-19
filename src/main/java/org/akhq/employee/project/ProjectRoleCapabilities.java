package org.akhq.employee.project;

import org.akhq.configs.security.Role;

import java.util.List;
import java.util.Map;

/**
 * What each project role can do, expressed as the same Resource/Action pairs the rest of the RBAC
 * module already uses. A project role is materialized into real PermissionGrant rows (see
 * ProjectService) rather than being a second thing the authorization layer has to understand -
 * EmployeeGrantSecurityRule and PermissionResolutionService stay exactly as already built.
 */
public final class ProjectRoleCapabilities {
    private ProjectRoleCapabilities() {
    }

    private record ResourceAction(Role.Resource resource, Role.Action action) {
    }

    private static final List<ResourceAction> VIEWER_CAPS = List.of(
        new ResourceAction(Role.Resource.TOPIC, Role.Action.READ),
        new ResourceAction(Role.Resource.TOPIC, Role.Action.READ_CONFIG),
        new ResourceAction(Role.Resource.TOPIC_DATA, Role.Action.READ),
        new ResourceAction(Role.Resource.CONSUMER_GROUP, Role.Action.READ),
        new ResourceAction(Role.Resource.CONNECT_CLUSTER, Role.Action.READ),
        new ResourceAction(Role.Resource.CONNECTOR, Role.Action.READ),
        new ResourceAction(Role.Resource.SCHEMA, Role.Action.READ),
        new ResourceAction(Role.Resource.NODE, Role.Action.READ),
        new ResourceAction(Role.Resource.ACL, Role.Action.READ),
        new ResourceAction(Role.Resource.KSQLDB, Role.Action.READ),
        new ResourceAction(Role.Resource.CLIENT_QUOTA, Role.Action.READ),
        new ResourceAction(Role.Resource.SCRAM_CREDENTIAL, Role.Action.READ),
        new ResourceAction(Role.Resource.TRANSACTION, Role.Action.READ)
    );

    private static final List<ResourceAction> DEVELOPER_EXTRA = List.of(
        new ResourceAction(Role.Resource.TOPIC, Role.Action.CREATE),
        new ResourceAction(Role.Resource.TOPIC_DATA, Role.Action.CREATE),
        new ResourceAction(Role.Resource.CONSUMER_GROUP, Role.Action.UPDATE_OFFSET),
        new ResourceAction(Role.Resource.CONSUMER_GROUP, Role.Action.DELETE_OFFSET),
        new ResourceAction(Role.Resource.KSQLDB, Role.Action.EXECUTE)
    );

    private static final List<ResourceAction> MAINTAINER_EXTRA = List.of(
        new ResourceAction(Role.Resource.CONNECTOR, Role.Action.CREATE),
        new ResourceAction(Role.Resource.CONNECTOR, Role.Action.UPDATE_STATE),
        new ResourceAction(Role.Resource.CONNECTOR, Role.Action.DELETE),
        new ResourceAction(Role.Resource.SCHEMA, Role.Action.CREATE),
        new ResourceAction(Role.Resource.SCHEMA, Role.Action.UPDATE),
        new ResourceAction(Role.Resource.TOPIC, Role.Action.ALTER_CONFIG),
        new ResourceAction(Role.Resource.CLIENT_QUOTA, Role.Action.ALTER_CONFIG),
        new ResourceAction(Role.Resource.TRANSACTION, Role.Action.DELETE)
    );

    private static final List<ResourceAction> OWNER_EXTRA = List.of(
        new ResourceAction(Role.Resource.TOPIC, Role.Action.DELETE),
        new ResourceAction(Role.Resource.TOPIC_DATA, Role.Action.DELETE),
        new ResourceAction(Role.Resource.CONSUMER_GROUP, Role.Action.DELETE),
        new ResourceAction(Role.Resource.SCHEMA, Role.Action.DELETE),
        new ResourceAction(Role.Resource.SCHEMA, Role.Action.DELETE_VERSION),
        new ResourceAction(Role.Resource.NODE, Role.Action.ALTER_CONFIG),
        new ResourceAction(Role.Resource.SCRAM_CREDENTIAL, Role.Action.CREATE),
        new ResourceAction(Role.Resource.SCRAM_CREDENTIAL, Role.Action.DELETE)
    );

    private static final Map<ProjectRole, List<ResourceAction>> BY_ROLE = Map.of(
        ProjectRole.VIEWER, VIEWER_CAPS,
        ProjectRole.DEVELOPER, concat(VIEWER_CAPS, DEVELOPER_EXTRA),
        ProjectRole.MAINTAINER, concat(concat(VIEWER_CAPS, DEVELOPER_EXTRA), MAINTAINER_EXTRA),
        ProjectRole.OWNER, concat(concat(concat(VIEWER_CAPS, DEVELOPER_EXTRA), MAINTAINER_EXTRA), OWNER_EXTRA)
    );

    private static List<ResourceAction> concat(List<ResourceAction> a, List<ResourceAction> b) {
        return java.util.stream.Stream.concat(a.stream(), b.stream()).distinct().toList();
    }

    public static List<Grant> grantsFor(ProjectRole role) {
        return BY_ROLE.get(role).stream()
            .map(ra -> new Grant(ra.resource(), ra.action()))
            .toList();
    }

    public record Grant(Role.Resource resource, Role.Action action) {
    }
}
