package org.akhq.employee.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Patch;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.akhq.employee.project.AddClusterRequest;
import org.akhq.employee.project.AddMemberRequest;
import org.akhq.employee.project.CreateProjectRequest;
import org.akhq.employee.project.ProjectAccessException;
import org.akhq.employee.project.ProjectRole;
import org.akhq.employee.project.ProjectService;
import org.akhq.employee.project.ProjectView;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Self-service - any authenticated employee can create a project (becoming its OWNER) and manage
 * projects they are a MAINTAINER or OWNER of. Not @Secured("ROLE_ADMIN"): authorization here is
 * per-project, enforced inside ProjectService against the caller's own ProjectMember row (a
 * platform ROLE_ADMIN bypasses this and can manage every project, same convention as the rest of
 * the admin module).
 */
@Secured(SecurityRule.IS_AUTHENTICATED)
@Controller("/api/projects")
public class ProjectController {
    @Inject
    private ProjectService projectService;

    @Error(exception = NoSuchElementException.class)
    public HttpResponse<?> notFound(NoSuchElementException e) {
        return HttpResponse.notFound(e.getMessage());
    }

    @Error(exception = IllegalArgumentException.class)
    public HttpResponse<?> badRequest(IllegalArgumentException e) {
        return HttpResponse.badRequest(e.getMessage());
    }

    @Error(exception = ProjectAccessException.class)
    public HttpResponse<?> forbidden(ProjectAccessException e) {
        return HttpResponse.status(HttpStatus.FORBIDDEN, e.getMessage());
    }

    @Get("/mine")
    @Operation(tags = {"projects"}, summary = "List projects the caller is a member of")
    public List<ProjectView> mine(Authentication authentication) {
        return projectService.listForEmployee(authentication.getName());
    }

    @Get
    @Operation(tags = {"projects"}, summary = "List every project (platform admin only - non-admins get their own via /mine)")
    public List<ProjectView> all(Authentication authentication) {
        return projectService.listAll(authentication.getName());
    }

    @Get("/{slug}")
    @Operation(tags = {"projects"}, summary = "Get one project, its members and linked clusters")
    public ProjectView get(String slug, Authentication authentication) {
        return projectService.get(slug, authentication.getName());
    }

    @Post(consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"projects"}, summary = "Create a project; the caller becomes its owner")
    public ProjectView create(@Body @Valid CreateProjectRequest body, Authentication authentication) {
        return projectService.create(body, authentication.getName());
    }

    @Post(value = "/{slug}/members", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"projects"}, summary = "Add a member to the project (requires MAINTAINER or above)")
    public ProjectView addMember(String slug, @Body @Valid AddMemberRequest body, Authentication authentication) {
        return projectService.addMember(slug, body, authentication.getName());
    }

    @Patch(value = "/{slug}/members/{employeeCode}", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"projects"}, summary = "Change a member's project role (requires OWNER)")
    public ProjectView changeMemberRole(String slug, String employeeCode, @Body ChangeRoleRequest body,
                                         Authentication authentication) {
        return projectService.changeMemberRole(slug, employeeCode, body.projectRole, authentication.getName());
    }

    @Delete("/{slug}/members/{employeeCode}")
    @Operation(tags = {"projects"}, summary = "Remove a member from the project (requires MAINTAINER or above)")
    public ProjectView removeMember(String slug, String employeeCode, Authentication authentication) {
        return projectService.removeMember(slug, employeeCode, authentication.getName());
    }

    @Post(value = "/{slug}/clusters", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"projects"}, summary = "Link an already-registered cluster to this project (requires MAINTAINER or above)")
    public ProjectView addCluster(String slug, @Body @Valid AddClusterRequest body, Authentication authentication) {
        return projectService.addCluster(slug, body, authentication.getName());
    }

    @Delete("/{slug}/clusters/{clusterName}")
    @Operation(tags = {"projects"}, summary = "Unlink a cluster from this project (requires MAINTAINER or above)")
    public ProjectView removeCluster(String slug, String clusterName, Authentication authentication) {
        return projectService.removeCluster(slug, clusterName, authentication.getName());
    }

    public static class ChangeRoleRequest {
        public ProjectRole projectRole;
    }
}
