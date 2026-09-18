package org.akhq.employee.service;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.employee.domain.Employee;
import org.akhq.employee.domain.EmployeeTeamMembership;
import org.akhq.employee.domain.PermissionGrant;
import org.akhq.employee.domain.Team;
import org.akhq.employee.domain.TeamPermissionGrant;
import org.akhq.employee.dto.EmployeeView;
import org.akhq.employee.dto.PermissionGrantRequest;
import org.akhq.employee.dto.PermissionGrantView;
import org.akhq.employee.dto.RbacExport;
import org.akhq.employee.repository.EmployeeRepository;
import org.akhq.employee.repository.EmployeeTeamMembershipRepository;
import org.akhq.employee.repository.PermissionGrantRepository;
import org.akhq.employee.repository.TeamPermissionGrantRepository;
import org.akhq.employee.repository.TeamRepository;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class RoleAssignmentService {
    private final EmployeeRepository employeeRepository;
    private final TeamRepository teamRepository;
    private final EmployeeTeamMembershipRepository membershipRepository;
    private final PermissionGrantRepository permissionGrantRepository;
    private final TeamPermissionGrantRepository teamPermissionGrantRepository;

    public RoleAssignmentService(EmployeeRepository employeeRepository,
                                  TeamRepository teamRepository,
                                  EmployeeTeamMembershipRepository membershipRepository,
                                  PermissionGrantRepository permissionGrantRepository,
                                  TeamPermissionGrantRepository teamPermissionGrantRepository) {
        this.employeeRepository = employeeRepository;
        this.teamRepository = teamRepository;
        this.membershipRepository = membershipRepository;
        this.permissionGrantRepository = permissionGrantRepository;
        this.teamPermissionGrantRepository = teamPermissionGrantRepository;
    }

    public List<EmployeeView> listEmployees() {
        return employeeRepository.findAllByOrderByFullNameAsc().stream()
            .map(this::toView)
            .collect(Collectors.toList());
    }

    public EmployeeView getEmployee(String employeeCode) {
        return toView(requireEmployee(employeeCode));
    }

    public void setAdmin(String employeeCode, boolean admin, String actingAdminCode) {
        guardAgainstActingOnSuperAdmin(employeeCode, actingAdminCode);
        requireEmployee(employeeCode);
        employeeRepository.updateAdminFlag(employeeCode, admin);
    }

    public void setSuperAdmin(String employeeCode, boolean superAdmin, String actingAdminCode) {
        requireSuperAdmin(actingAdminCode);
        requireEmployee(employeeCode);
        employeeRepository.updateSuperAdminFlag(employeeCode, superAdmin);
    }

    public void setActive(String employeeCode, boolean active, String actingAdminCode) {
        guardAgainstActingOnSuperAdmin(employeeCode, actingAdminCode);
        requireEmployee(employeeCode);
        employeeRepository.updateActiveFlag(employeeCode, active);
    }

    /**
     * A regular admin has the same day-to-day permissions as a super admin (teams, grants,
     * settings) but cannot promote, demote, deactivate or otherwise act on a super admin account.
     * Only another super admin can touch a super admin's own account state.
     */
    private void guardAgainstActingOnSuperAdmin(String targetEmployeeCode, String actingAdminCode) {
        Employee target = requireEmployee(targetEmployeeCode);
        if (!target.isSuperAdmin()) {
            return;
        }
        Employee actingAdmin = requireEmployee(actingAdminCode);
        if (!actingAdmin.isSuperAdmin()) {
            throw new AdminHierarchyException(
                "Only a super admin can change another super admin's account: " + targetEmployeeCode);
        }
    }

    private void requireSuperAdmin(String actingAdminCode) {
        Employee actingAdmin = requireEmployee(actingAdminCode);
        if (!actingAdmin.isSuperAdmin()) {
            throw new AdminHierarchyException("Only a super admin can grant or revoke super admin access");
        }
    }

    public void assignTeam(String employeeCode, String teamName) {
        Employee employee = requireEmployee(employeeCode);
        Team team = requireTeam(teamName);
        if (membershipRepository.findByEmployeeIdAndTeamId(employee.getId(), team.getId()).isEmpty()) {
            membershipRepository.save(new EmployeeTeamMembership(employee.getId(), team.getId()));
        }
    }

    public void removeTeam(String employeeCode, String teamName) {
        Employee employee = requireEmployee(employeeCode);
        Team team = requireTeam(teamName);
        membershipRepository.deleteByEmployeeIdAndTeamId(employee.getId(), team.getId());
    }

    public PermissionGrantView addEmployeeGrant(String employeeCode, PermissionGrantRequest request, String grantedBy) {
        Employee employee = requireEmployee(employeeCode);
        PermissionGrant grant = new PermissionGrant(
            employee.getId(), request.resource, request.action,
            request.clusterPattern, request.topicPattern, grantedBy
        );
        return PermissionGrantView.fromEmployeeGrant(permissionGrantRepository.save(grant));
    }

    public void revokeEmployeeGrant(String employeeCode, Long grantId) {
        Employee employee = requireEmployee(employeeCode);
        permissionGrantRepository.deleteByIdAndEmployeeId(grantId, employee.getId());
    }

    public List<Team> listTeams() {
        return teamRepository.findAllByOrderByNameAsc();
    }

    public Team createTeam(String name, String description) {
        return teamRepository.findByName(name).orElseGet(() -> {
            Team team = new Team();
            team.setName(name);
            team.setDescription(description);
            return teamRepository.save(team);
        });
    }

    public PermissionGrantView addTeamGrant(String teamName, PermissionGrantRequest request, String grantedBy) {
        Team team = requireTeam(teamName);
        TeamPermissionGrant grant = new TeamPermissionGrant(
            team.getId(), request.resource, request.action,
            request.clusterPattern, request.topicPattern, grantedBy
        );
        return PermissionGrantView.fromTeamGrant(teamPermissionGrantRepository.save(grant), teamName);
    }

    public void revokeTeamGrant(String teamName, Long grantId) {
        Team team = requireTeam(teamName);
        teamPermissionGrantRepository.deleteByIdAndTeamId(grantId, team.getId());
    }

    public RbacExport export() {
        List<Team> teams = teamRepository.findAllByOrderByNameAsc();
        Map<Long, Team> teamsById = teams.stream().collect(Collectors.toMap(Team::getId, t -> t));

        List<RbacExport.TeamExport> teamExports = teams.stream()
            .map(team -> new RbacExport.TeamExport(
                team.getName(),
                team.getDescription(),
                teamPermissionGrantRepository.findByTeamId(team.getId()).stream()
                    .map(g -> PermissionGrantView.fromTeamGrant(g, team.getName()))
                    .collect(Collectors.toList())
            ))
            .collect(Collectors.toList());

        List<RbacExport.EmployeeExport> employeeExports = employeeRepository.findAllByOrderByFullNameAsc().stream()
            .map(employee -> {
                List<String> teamNames = membershipRepository.findByEmployeeId(employee.getId()).stream()
                    .map(m -> teamsById.get(m.getTeamId()))
                    .filter(t -> t != null)
                    .map(Team::getName)
                    .collect(Collectors.toList());
                List<PermissionGrantView> grants = permissionGrantRepository.findByEmployeeId(employee.getId()).stream()
                    .map(PermissionGrantView::fromEmployeeGrant)
                    .collect(Collectors.toList());
                return new RbacExport.EmployeeExport(
                    employee.getEmployeeCode(), employee.getFullName(), employee.isAdmin(), employee.isSuperAdmin(),
                    employee.isActive(), teamNames, grants
                );
            })
            .collect(Collectors.toList());

        return new RbacExport(employeeExports, teamExports);
    }

    public void importData(RbacExport export, String importedBy) {
        Employee actingAdmin = requireEmployee(importedBy);

        // Validate every record before writing any of them: an import must not be a side door
        // around the admin hierarchy that setAdmin/setSuperAdmin/setActive already enforce.
        for (RbacExport.EmployeeExport employeeExport : export.employees()) {
            if ((employeeExport.admin() || employeeExport.superAdmin()) && !actingAdmin.isSuperAdmin()) {
                throw new AdminHierarchyException(
                    "Only a super admin can import an employee with admin or super admin access: "
                        + employeeExport.employeeCode());
            }
            Optional<Employee> existing = employeeRepository.findByEmployeeCode(employeeExport.employeeCode());
            if (existing.isPresent() && existing.get().isSuperAdmin() && !actingAdmin.isSuperAdmin()) {
                throw new AdminHierarchyException(
                    "Only a super admin can modify an existing super admin via import: "
                        + employeeExport.employeeCode());
            }
        }

        for (RbacExport.TeamExport teamExport : export.teams()) {
            Team team = createTeam(teamExport.name(), teamExport.description());
            for (PermissionGrantView grant : teamExport.grants()) {
                teamPermissionGrantRepository.save(new TeamPermissionGrant(
                    team.getId(), grant.resource(), grant.action(),
                    grant.clusterPattern(), grant.topicPattern(), importedBy
                ));
            }
        }

        for (RbacExport.EmployeeExport employeeExport : export.employees()) {
            Employee employee = employeeRepository.findByEmployeeCode(employeeExport.employeeCode())
                .orElseGet(() -> new Employee(employeeExport.employeeCode(), employeeExport.fullName()));
            employee.setFullName(employeeExport.fullName());
            employee.setAdmin(employeeExport.admin());
            employee.setSuperAdmin(employeeExport.superAdmin());
            employee.setActive(employeeExport.active());
            Employee saved = employee.getId() == null
                ? employeeRepository.save(employee)
                : employeeRepository.update(employee);

            for (String teamName : employeeExport.teams()) {
                assignTeam(saved.getEmployeeCode(), teamName);
            }
            for (PermissionGrantView grant : employeeExport.grants()) {
                permissionGrantRepository.save(new PermissionGrant(
                    saved.getId(), grant.resource(), grant.action(),
                    grant.clusterPattern(), grant.topicPattern(), importedBy
                ));
            }
        }
    }

    private EmployeeView toView(Employee employee) {
        List<String> teamNames = membershipRepository.findByEmployeeId(employee.getId()).stream()
            .map(m -> teamRepository.findById(m.getTeamId()).map(Team::getName).orElse(null))
            .filter(name -> name != null)
            .collect(Collectors.toList());

        List<PermissionGrantView> grants = permissionGrantRepository.findByEmployeeId(employee.getId()).stream()
            .map(PermissionGrantView::fromEmployeeGrant)
            .collect(Collectors.toList());

        return EmployeeView.from(employee, teamNames, grants);
    }

    private Employee requireEmployee(String employeeCode) {
        return employeeRepository.findByEmployeeCode(employeeCode)
            .orElseThrow(() -> new NoSuchElementException("Employee not found: " + employeeCode));
    }

    private Team requireTeam(String teamName) {
        return teamRepository.findByName(teamName)
            .orElseThrow(() -> new NoSuchElementException("Team not found: " + teamName));
    }
}
