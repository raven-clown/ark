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
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.akhq.employee.domain.Team;
import org.akhq.employee.dto.AssignTeamRequest;
import org.akhq.employee.dto.EmployeeView;
import org.akhq.employee.dto.PermissionGrantRequest;
import org.akhq.employee.dto.PermissionGrantView;
import org.akhq.employee.dto.ManualAccountRequest;
import org.akhq.employee.dto.PasswordResetRequest;
import org.akhq.employee.dto.RbacExport;
import org.akhq.employee.domain.Employee;
import org.akhq.employee.service.AdminHierarchyException;
import org.akhq.employee.service.ManualAccountService;
import org.akhq.employee.service.RetentionSettingsService;
import org.akhq.employee.service.RoleAssignmentService;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@Secured("ROLE_ADMIN")
@Controller("/api/admin")
public class AdminEmployeeController {
    @Inject
    private RoleAssignmentService roleAssignmentService;
    @Inject
    private ManualAccountService manualAccountService;
    @Inject
    private RetentionSettingsService retentionSettingsService;

    @Error(exception = NoSuchElementException.class)
    public HttpResponse<?> notFound(NoSuchElementException e) {
        return HttpResponse.notFound(e.getMessage());
    }

    @Error(exception = IllegalArgumentException.class)
    public HttpResponse<?> badRequest(IllegalArgumentException e) {
        return HttpResponse.badRequest(e.getMessage());
    }

    @Error(exception = AdminHierarchyException.class)
    public HttpResponse<?> forbidden(AdminHierarchyException e) {
        return HttpResponse.status(HttpStatus.FORBIDDEN, e.getMessage());
    }

    @Post(value = "/employees/manual", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Create an account manually with a password, independent of the directory")
    public EmployeeView createManualAccount(@Body @Valid ManualAccountRequest body) {
        Employee employee = manualAccountService.createAccount(
            body.employeeCode, body.fullName, body.password, body.admin
        );
        return roleAssignmentService.getEmployee(employee.getEmployeeCode());
    }

    @Patch(value = "/employees/{employeeCode}/password", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Reset the password of a manually created account")
    public HttpResponse<?> resetPassword(String employeeCode, @Body @Valid PasswordResetRequest body) {
        manualAccountService.resetPassword(employeeCode, body.password);
        return HttpResponse.noContent();
    }

    @Get("/employees")
    @Operation(tags = {"admin"}, summary = "List every employee pulled in from the directory")
    public List<EmployeeView> listEmployees() {
        return roleAssignmentService.listEmployees();
    }

    @Get("/employees/{employeeCode}")
    @Operation(tags = {"admin"}, summary = "Get one employee with teams and grants")
    public EmployeeView getEmployee(String employeeCode) {
        return roleAssignmentService.getEmployee(employeeCode);
    }

    @Patch("/employees/{employeeCode}/admin")
    @Operation(tags = {"admin"}, summary = "Grant or revoke admin access")
    public HttpResponse<?> setAdmin(String employeeCode, @Body AdminFlagRequest body, Authentication authentication) {
        roleAssignmentService.setAdmin(employeeCode, body.admin, currentAdminCode(authentication));
        return HttpResponse.noContent();
    }

    @Patch("/employees/{employeeCode}/super-admin")
    @Operation(tags = {"admin"}, summary = "Grant or revoke super admin access, only a super admin may do this")
    public HttpResponse<?> setSuperAdmin(String employeeCode, @Body AdminFlagRequest body, Authentication authentication) {
        roleAssignmentService.setSuperAdmin(employeeCode, body.admin, currentAdminCode(authentication));
        return HttpResponse.noContent();
    }

    @Patch("/employees/{employeeCode}/active")
    @Operation(tags = {"admin"}, summary = "Activate or deactivate an employee account")
    public HttpResponse<?> setActive(String employeeCode, @Body ActiveFlagRequest body, Authentication authentication) {
        roleAssignmentService.setActive(employeeCode, body.active, currentAdminCode(authentication));
        return HttpResponse.noContent();
    }

    @Get("/settings/retention")
    @Operation(tags = {"admin"}, summary = "List data retention settings, ISO-aligned defaults, editable by any admin")
    public Map<String, String> listRetentionSettings() {
        return retentionSettingsService.listAll();
    }

    @Patch(value = "/settings/retention/{key}", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Update a data retention setting, in days")
    public HttpResponse<?> updateRetentionSetting(String key, @Body RetentionValueRequest body, Authentication authentication) {
        retentionSettingsService.update(key, String.valueOf(body.days), currentAdminCode(authentication));
        return HttpResponse.noContent();
    }

    @Post("/employees/{employeeCode}/teams")
    @Operation(tags = {"admin"}, summary = "Assign an employee to a team")
    public HttpResponse<?> assignTeam(String employeeCode, @Body @Valid AssignTeamRequest body) {
        roleAssignmentService.assignTeam(employeeCode, body.teamName);
        return HttpResponse.noContent();
    }

    @Delete("/employees/{employeeCode}/teams/{teamName}")
    @Operation(tags = {"admin"}, summary = "Remove an employee from a team")
    public HttpResponse<?> removeTeam(String employeeCode, String teamName) {
        roleAssignmentService.removeTeam(employeeCode, teamName);
        return HttpResponse.noContent();
    }

    @Post(value = "/employees/{employeeCode}/grants", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Grant a permission directly to an employee")
    public PermissionGrantView addEmployeeGrant(String employeeCode, @Body @Valid PermissionGrantRequest body,
                                                 Authentication authentication) {
        return roleAssignmentService.addEmployeeGrant(employeeCode, body, currentAdminCode(authentication));
    }

    @Delete("/employees/{employeeCode}/grants/{grantId}")
    @Operation(tags = {"admin"}, summary = "Revoke a permission previously granted to an employee")
    public HttpResponse<?> revokeEmployeeGrant(String employeeCode, Long grantId) {
        roleAssignmentService.revokeEmployeeGrant(employeeCode, grantId);
        return HttpResponse.noContent();
    }

    @Get("/teams")
    @Operation(tags = {"admin"}, summary = "List every team")
    public List<Team> listTeams() {
        return roleAssignmentService.listTeams();
    }

    @Post("/teams")
    @Operation(tags = {"admin"}, summary = "Create a team")
    public Team createTeam(@Body @Valid AssignTeamRequest body) {
        return roleAssignmentService.createTeam(body.teamName, null);
    }

    @Post(value = "/teams/{teamName}/grants", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Grant a permission to every member of a team")
    public PermissionGrantView addTeamGrant(String teamName, @Body @Valid PermissionGrantRequest body,
                                             Authentication authentication) {
        return roleAssignmentService.addTeamGrant(teamName, body, currentAdminCode(authentication));
    }

    @Delete("/teams/{teamName}/grants/{grantId}")
    @Operation(tags = {"admin"}, summary = "Revoke a team-level permission grant")
    public HttpResponse<?> revokeTeamGrant(String teamName, Long grantId) {
        roleAssignmentService.revokeTeamGrant(teamName, grantId);
        return HttpResponse.noContent();
    }

    @Get("/rbac/export")
    @Operation(tags = {"admin"}, summary = "Export every employee, team and grant as JSON for backup")
    public RbacExport export() {
        return roleAssignmentService.export();
    }

    @Post(value = "/rbac/import", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Bulk import employees, teams and grants from a previous export")
    public HttpResponse<?> importRbac(@Body RbacExport body, Authentication authentication) {
        roleAssignmentService.importData(body, currentAdminCode(authentication));
        return HttpResponse.noContent();
    }

    private String currentAdminCode(Authentication authentication) {
        return authentication != null ? authentication.getName() : "unknown";
    }

    public static class AdminFlagRequest {
        public boolean admin;
    }

    public static class ActiveFlagRequest {
        public boolean active;
    }

    public static class RetentionValueRequest {
        public int days;
    }
}
