package org.akhq.employee.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.akhq.configs.security.Role;
import org.akhq.controllers.AbstractController;
import org.akhq.employee.quota.ClientQuotaEntityType;
import org.akhq.employee.quota.ClientQuotaEntry;
import org.akhq.employee.quota.ClientQuotaRequest;
import org.akhq.employee.quota.ClientQuotaService;
import org.akhq.security.annotation.AKHQSecured;

import java.util.List;

@Controller("/api/{cluster}/client-quota")
public class ClientQuotaController extends AbstractController {
    @Inject
    private ClientQuotaService clientQuotaService;

    @AKHQSecured(resource = Role.Resource.CLIENT_QUOTA, action = Role.Action.READ)
    @Get
    @Operation(tags = {"client quota"}, summary = "List client quotas on a cluster")
    public List<ClientQuotaEntry> list(String cluster) throws Exception {
        checkIfClusterAllowed(cluster);
        return clientQuotaService.list(cluster);
    }

    @AKHQSecured(resource = Role.Resource.CLIENT_QUOTA, action = Role.Action.ALTER_CONFIG)
    @Post(consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"client quota"}, summary = "Set a client quota on a cluster")
    public HttpResponse<?> set(String cluster, @Body @Valid ClientQuotaRequest body) throws Exception {
        checkIfClusterAllowed(cluster);
        clientQuotaService.set(cluster, body);
        return HttpResponse.noContent();
    }

    @AKHQSecured(resource = Role.Resource.CLIENT_QUOTA, action = Role.Action.ALTER_CONFIG)
    @Delete("/{entityType}/{entityName}")
    @Operation(tags = {"client quota"}, summary = "Remove a client quota from a cluster")
    public HttpResponse<?> delete(String cluster, ClientQuotaEntityType entityType, String entityName) throws Exception {
        checkIfClusterAllowed(cluster);
        clientQuotaService.delete(cluster, entityType, entityName);
        return HttpResponse.noContent();
    }
}
