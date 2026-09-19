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
import org.akhq.employee.scram.ScramCredentialEntry;
import org.akhq.employee.scram.ScramCredentialRequest;
import org.akhq.employee.scram.ScramCredentialService;
import org.akhq.employee.scram.ScramMechanismChoice;
import org.akhq.security.annotation.AKHQSecured;

import java.util.List;

@Controller("/api/{cluster}/scram-credential")
public class ScramCredentialController extends AbstractController {
    @Inject
    private ScramCredentialService scramCredentialService;

    @AKHQSecured(resource = Role.Resource.SCRAM_CREDENTIAL, action = Role.Action.READ)
    @Get
    @Operation(tags = {"scram credential"}, summary = "List SASL/SCRAM credentials on a cluster")
    public List<ScramCredentialEntry> list(String cluster) throws Exception {
        checkIfClusterAllowed(cluster);
        return scramCredentialService.list(cluster);
    }

    @AKHQSecured(resource = Role.Resource.SCRAM_CREDENTIAL, action = Role.Action.CREATE)
    @Post(consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"scram credential"}, summary = "Create or rotate a SASL/SCRAM credential")
    public HttpResponse<?> set(String cluster, @Body @Valid ScramCredentialRequest body) throws Exception {
        checkIfClusterAllowed(cluster);
        scramCredentialService.set(cluster, body);
        return HttpResponse.noContent();
    }

    @AKHQSecured(resource = Role.Resource.SCRAM_CREDENTIAL, action = Role.Action.DELETE)
    @Delete("/{username}/{mechanism}")
    @Operation(tags = {"scram credential"}, summary = "Remove a SASL/SCRAM credential")
    public HttpResponse<?> delete(String cluster, String username, ScramMechanismChoice mechanism) throws Exception {
        checkIfClusterAllowed(cluster);
        scramCredentialService.delete(cluster, username, mechanism);
        return HttpResponse.noContent();
    }
}
