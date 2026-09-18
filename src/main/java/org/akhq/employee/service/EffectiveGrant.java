package org.akhq.employee.service;

import org.akhq.configs.security.Role;

public record EffectiveGrant(Role.Resource resource, Role.Action action, String clusterPattern, String topicPattern) {
    public boolean matches(Role.Resource resource, Role.Action action, String cluster, String topic) {
        return this.resource == resource
            && this.action == action
            && cluster.matches(clusterPattern)
            && (topic == null || topic.matches(topicPattern));
    }
}
