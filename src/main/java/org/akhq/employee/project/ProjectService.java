package org.akhq.employee.project;

import jakarta.inject.Singleton;
import org.akhq.employee.domain.Employee;
import org.akhq.employee.domain.PermissionGrant;
import org.akhq.employee.repository.EmployeeRepository;
import org.akhq.employee.repository.PermissionGrantRepository;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Project is a logical grouping only - who is on the team, which already-registered clusters
 * they use, and what each member can do on those clusters. It does not provision any
 * infrastructure. Membership and cluster links are materialized into real PermissionGrant rows
 * (tagged "project:<id>:<cluster>") so the existing, already-audited enforcement path
 * (EmployeeGrantSecurityRule / PermissionResolutionService) needs no changes at all to respect
 * project boundaries.
 */
@Singleton
public class ProjectService {
    private static final Pattern SAFE_SLUG = Pattern.compile("^[a-z0-9-]+$");

    private final ProjectRepository projectRepository;
    private final ProjectMemberRepository memberRepository;
    private final ProjectClusterRepository clusterRepository;
    private final EmployeeRepository employeeRepository;
    private final PermissionGrantRepository permissionGrantRepository;

    public ProjectService(ProjectRepository projectRepository, ProjectMemberRepository memberRepository,
                           ProjectClusterRepository clusterRepository, EmployeeRepository employeeRepository,
                           PermissionGrantRepository permissionGrantRepository) {
        this.projectRepository = projectRepository;
        this.memberRepository = memberRepository;
        this.clusterRepository = clusterRepository;
        this.employeeRepository = employeeRepository;
        this.permissionGrantRepository = permissionGrantRepository;
    }

    public List<ProjectView> listAll(String actingEmployeeCode) {
        Employee actingEmployee = requireEmployee(actingEmployeeCode);
        if (!actingEmployee.isAdmin()) {
            throw new ProjectAccessException("Only a platform admin can list every project");
        }
        return projectRepository.findAllByOrderByNameAsc().stream()
            .map(p -> toView(p, null))
            .collect(Collectors.toList());
    }

    public List<ProjectView> listForEmployee(String employeeCode) {
        Employee employee = requireEmployee(employeeCode);
        List<Long> projectIds = memberRepository.findByEmployeeId(employee.getId()).stream()
            .map(ProjectMember::getProjectId)
            .collect(Collectors.toList());
        return projectIds.stream()
            .map(id -> projectRepository.findById(id).orElse(null))
            .filter(p -> p != null)
            .map(p -> toView(p, employeeCode))
            .collect(Collectors.toList());
    }

    public ProjectView get(String slug, String callerEmployeeCode) {
        Project project = requireProject(slug);
        requireProjectRole(project, callerEmployeeCode, ProjectRole.VIEWER);
        return toView(project, callerEmployeeCode);
    }

    public ProjectView create(CreateProjectRequest request, String creatorEmployeeCode) {
        if (!SAFE_SLUG.matcher(request.slug).matches()) {
            throw new IllegalArgumentException(
                "Project slug must be lowercase letters, digits and '-' only: " + request.slug);
        }
        if (projectRepository.findBySlug(request.slug).isPresent()) {
            throw new IllegalArgumentException("A project with slug " + request.slug + " already exists");
        }

        Employee creator = requireEmployee(creatorEmployeeCode);
        Project project = projectRepository.save(
            new Project(request.name, request.slug, request.description, creatorEmployeeCode));

        memberRepository.save(new ProjectMember(project.getId(), creator.getId(), ProjectRole.OWNER, creatorEmployeeCode));
        // No clusters linked yet, so nothing to materialize - grants get created as clusters are added.

        return toView(project, creatorEmployeeCode);
    }

    public ProjectView addMember(String slug, AddMemberRequest request, String actingEmployeeCode) {
        Project project = requireProject(slug);
        requireProjectRole(project, actingEmployeeCode, ProjectRole.MAINTAINER);

        Employee target = requireEmployee(request.employeeCode);
        if (memberRepository.findByProjectIdAndEmployeeId(project.getId(), target.getId()).isPresent()) {
            throw new IllegalArgumentException(request.employeeCode + " is already a member of this project");
        }

        memberRepository.save(new ProjectMember(project.getId(), target.getId(), request.projectRole, actingEmployeeCode));
        materializeGrantsForMember(project, target, request.projectRole);

        return toView(project, actingEmployeeCode);
    }

    public ProjectView changeMemberRole(String slug, String employeeCode, ProjectRole newRole, String actingEmployeeCode) {
        Project project = requireProject(slug);
        requireProjectRole(project, actingEmployeeCode, ProjectRole.OWNER);

        Employee target = requireEmployee(employeeCode);
        ProjectMember member = memberRepository.findByProjectIdAndEmployeeId(project.getId(), target.getId())
            .orElseThrow(() -> new NoSuchElementException(employeeCode + " is not a member of this project"));

        if (member.getProjectRole() == ProjectRole.OWNER && newRole != ProjectRole.OWNER) {
            requireAnotherOwnerExists(project, target.getId());
        }

        member.setProjectRole(newRole);
        memberRepository.update(member);

        revokeGrantsForMember(project, target);
        materializeGrantsForMember(project, target, newRole);

        return toView(project, actingEmployeeCode);
    }

    public ProjectView removeMember(String slug, String employeeCode, String actingEmployeeCode) {
        Project project = requireProject(slug);
        requireProjectRole(project, actingEmployeeCode, ProjectRole.MAINTAINER);

        Employee target = requireEmployee(employeeCode);
        ProjectMember member = memberRepository.findByProjectIdAndEmployeeId(project.getId(), target.getId())
            .orElseThrow(() -> new NoSuchElementException(employeeCode + " is not a member of this project"));

        if (member.getProjectRole() == ProjectRole.OWNER) {
            requireAnotherOwnerExists(project, target.getId());
        }

        memberRepository.deleteByProjectIdAndEmployeeId(project.getId(), target.getId());
        revokeGrantsForMember(project, target);

        return toView(project, actingEmployeeCode);
    }

    public ProjectView addCluster(String slug, AddClusterRequest request, String actingEmployeeCode) {
        Project project = requireProject(slug);
        requireProjectRole(project, actingEmployeeCode, ProjectRole.MAINTAINER);

        if (clusterRepository.findByProjectIdAndClusterName(project.getId(), request.clusterName).isPresent()) {
            throw new IllegalArgumentException(request.clusterName + " is already linked to this project");
        }

        clusterRepository.save(new ProjectCluster(project.getId(), request.clusterName, actingEmployeeCode));

        for (ProjectMember member : memberRepository.findByProjectId(project.getId())) {
            Employee employee = employeeRepository.findById(member.getEmployeeId()).orElse(null);
            if (employee != null) {
                materializeGrantsForCluster(project, employee, member.getProjectRole(), request.clusterName);
            }
        }

        return toView(project, actingEmployeeCode);
    }

    public ProjectView removeCluster(String slug, String clusterName, String actingEmployeeCode) {
        Project project = requireProject(slug);
        requireProjectRole(project, actingEmployeeCode, ProjectRole.MAINTAINER);

        clusterRepository.findByProjectIdAndClusterName(project.getId(), clusterName)
            .orElseThrow(() -> new NoSuchElementException(clusterName + " is not linked to this project"));

        clusterRepository.deleteByProjectIdAndClusterName(project.getId(), clusterName);
        permissionGrantRepository.deleteByGrantedByStartingWith(grantTagPrefix(project) + clusterName);

        return toView(project, actingEmployeeCode);
    }

    private void materializeGrantsForMember(Project project, Employee employee, ProjectRole role) {
        for (ProjectCluster cluster : clusterRepository.findByProjectId(project.getId())) {
            materializeGrantsForCluster(project, employee, role, cluster.getClusterName());
        }
    }

    private void materializeGrantsForCluster(Project project, Employee employee, ProjectRole role, String clusterName) {
        String tag = grantTagPrefix(project) + clusterName;
        for (ProjectRoleCapabilities.Grant grant : ProjectRoleCapabilities.grantsFor(role)) {
            permissionGrantRepository.save(new PermissionGrant(
                employee.getId(), grant.resource(), grant.action(), clusterName, ".*", tag
            ));
        }
    }

    private void revokeGrantsForMember(Project project, Employee employee) {
        permissionGrantRepository.deleteByEmployeeIdAndGrantedByStartingWith(
            employee.getId(), grantTagPrefix(project));
    }

    private String grantTagPrefix(Project project) {
        return "project:" + project.getId() + ":";
    }

    private void requireAnotherOwnerExists(Project project, Long excludingEmployeeId) {
        boolean anotherOwner = memberRepository.findByProjectId(project.getId()).stream()
            .anyMatch(m -> m.getProjectRole() == ProjectRole.OWNER && !m.getEmployeeId().equals(excludingEmployeeId));
        if (!anotherOwner) {
            throw new IllegalArgumentException("A project must always have at least one owner");
        }
    }

    private void requireProjectRole(Project project, String actingEmployeeCode, ProjectRole minimum) {
        Employee actingEmployee = requireEmployee(actingEmployeeCode);
        if (actingEmployee.isAdmin()) {
            return;
        }
        ProjectMember member = memberRepository.findByProjectIdAndEmployeeId(project.getId(), actingEmployee.getId())
            .orElseThrow(() -> new ProjectAccessException("You are not a member of this project"));
        if (!member.getProjectRole().atLeast(minimum)) {
            throw new ProjectAccessException(
                "This action requires at least " + minimum + " on this project");
        }
    }

    private ProjectView toView(Project project, String callerEmployeeCode) {
        List<ProjectMember> members = memberRepository.findByProjectId(project.getId());
        List<ProjectMemberView> memberViews = members.stream()
            .map(m -> employeeRepository.findById(m.getEmployeeId())
                .map(e -> new ProjectMemberView(e.getEmployeeCode(), e.getFullName(), m.getProjectRole()))
                .orElse(null))
            .filter(v -> v != null)
            .collect(Collectors.toList());

        List<String> clusters = clusterRepository.findByProjectId(project.getId()).stream()
            .map(ProjectCluster::getClusterName)
            .collect(Collectors.toList());

        ProjectRole callerRole = null;
        if (callerEmployeeCode != null) {
            Optional<Employee> caller = employeeRepository.findByEmployeeCode(callerEmployeeCode);
            if (caller.isPresent()) {
                callerRole = members.stream()
                    .filter(m -> m.getEmployeeId().equals(caller.get().getId()))
                    .map(ProjectMember::getProjectRole)
                    .findFirst()
                    .orElse(null);
            }
        }

        return new ProjectView(
            project.getId(), project.getName(), project.getSlug(), project.getDescription(),
            project.getCreatedBy(), project.getCreatedAt(), memberViews, clusters, callerRole
        );
    }

    private Project requireProject(String slug) {
        return projectRepository.findBySlug(slug)
            .orElseThrow(() -> new NoSuchElementException("Project not found: " + slug));
    }

    private Employee requireEmployee(String employeeCode) {
        return employeeRepository.findByEmployeeCode(employeeCode)
            .orElseThrow(() -> new NoSuchElementException("Employee not found: " + employeeCode));
    }
}
