package org.akhq.employee.service;

import jakarta.inject.Singleton;
import org.akhq.employee.domain.PermissionGrant;
import org.akhq.employee.domain.TeamPermissionGrant;
import org.akhq.employee.repository.EmployeeTeamMembershipRepository;
import org.akhq.employee.repository.PermissionGrantRepository;
import org.akhq.employee.repository.TeamPermissionGrantRepository;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class PermissionResolutionService {
    private final PermissionGrantRepository permissionGrantRepository;
    private final TeamPermissionGrantRepository teamPermissionGrantRepository;
    private final EmployeeTeamMembershipRepository membershipRepository;

    public PermissionResolutionService(PermissionGrantRepository permissionGrantRepository,
                                        TeamPermissionGrantRepository teamPermissionGrantRepository,
                                        EmployeeTeamMembershipRepository membershipRepository) {
        this.permissionGrantRepository = permissionGrantRepository;
        this.teamPermissionGrantRepository = teamPermissionGrantRepository;
        this.membershipRepository = membershipRepository;
    }

    public List<EffectiveGrant> resolveEffectiveGrants(Long employeeId) {
        List<PermissionGrant> individual = permissionGrantRepository.findByEmployeeId(employeeId);

        List<Long> teamIds = membershipRepository.findByEmployeeId(employeeId).stream()
            .map(m -> m.getTeamId())
            .collect(Collectors.toList());

        List<TeamPermissionGrant> fromTeams = teamIds.isEmpty()
            ? List.of()
            : teamPermissionGrantRepository.findByTeamIdIn(teamIds);

        return Stream.concat(
            individual.stream().map(g -> new EffectiveGrant(g.getResource(), g.getAction(), g.getClusterPattern(), g.getTopicPattern())),
            fromTeams.stream().map(g -> new EffectiveGrant(g.getResource(), g.getAction(), g.getClusterPattern(), g.getTopicPattern()))
        ).distinct().collect(Collectors.toList());
    }
}
