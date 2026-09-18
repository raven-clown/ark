package org.akhq.employee.project;

public enum ProjectRole {
    VIEWER(0),
    DEVELOPER(1),
    MAINTAINER(2),
    OWNER(3);

    private final int rank;

    ProjectRole(int rank) {
        this.rank = rank;
    }

    public boolean atLeast(ProjectRole other) {
        return this.rank >= other.rank;
    }
}
