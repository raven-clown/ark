package org.akhq.employee.domain;

import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import lombok.Data;
import lombok.NoArgsConstructor;

@MappedEntity("employee_teams")
@Data
@NoArgsConstructor
public class EmployeeTeamMembership {
    @Id
    @GeneratedValue
    private Long id;

    private Long employeeId;
    private Long teamId;

    public EmployeeTeamMembership(Long employeeId, Long teamId) {
        this.employeeId = employeeId;
        this.teamId = teamId;
    }
}
