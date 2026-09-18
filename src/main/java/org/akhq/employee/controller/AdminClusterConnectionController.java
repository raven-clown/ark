package org.akhq.employee.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.akhq.employee.cluster.ClusterConnectionRequest;
import org.akhq.employee.cluster.ClusterConnectionService;
import org.akhq.employee.cluster.ClusterConnectionView;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Manages cluster connection definitions. Saving one here does NOT make the cluster reachable by
 * itself - copy the returned yamlSnippet into application.yml under akhq.connections and restart,
 * until the KafkaModule wiring described in the architecture blueprint lands.
 */
@Secured("ROLE_ADMIN")
@Controller("/api/admin/cluster-connections")
public class AdminClusterConnectionController {
    @Inject
    private ClusterConnectionService clusterConnectionService;

    @Error(exception = NoSuchElementException.class)
    public HttpResponse<?> notFound(NoSuchElementException e) {
        return HttpResponse.notFound(e.getMessage());
    }

    @Error(exception = IllegalArgumentException.class)
    public HttpResponse<?> badRequest(IllegalArgumentException e) {
        return HttpResponse.badRequest(e.getMessage());
    }

    @Get
    @Operation(tags = {"admin"}, summary = "List cluster connection definitions")
    public List<ClusterConnectionView> list() {
        return clusterConnectionService.list();
    }

    @Get("/{name}")
    @Operation(tags = {"admin"}, summary = "Get one cluster connection definition, with its generated YAML")
    public ClusterConnectionView get(String name) {
        return clusterConnectionService.get(name);
    }

    @Post(consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Define a new cluster connection")
    public ClusterConnectionView create(@Body @Valid ClusterConnectionRequest body, Authentication authentication) {
        return clusterConnectionService.create(body, authentication != null ? authentication.getName() : "unknown");
    }

    @Put(value = "/{name}", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Update a cluster connection definition")
    public ClusterConnectionView update(String name, @Body @Valid ClusterConnectionRequest body) {
        return clusterConnectionService.update(name, body);
    }

    @Delete("/{name}")
    @Operation(tags = {"admin"}, summary = "Delete a cluster connection definition")
    public HttpResponse<?> delete(String name) {
        clusterConnectionService.delete(name);
        return HttpResponse.noContent();
    }
}
