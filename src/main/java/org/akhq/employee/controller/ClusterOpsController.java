package org.akhq.employee.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.akhq.configs.security.Role;
import org.akhq.controllers.AbstractController;
import org.akhq.employee.clusterops.AbortTransactionRequest;
import org.akhq.employee.clusterops.ClusterOpsService;
import org.akhq.employee.clusterops.LogDirEntry;
import org.akhq.employee.clusterops.QuorumView;
import org.akhq.employee.clusterops.TransactionDetail;
import org.akhq.employee.clusterops.TransactionSummary;
import org.akhq.security.annotation.AKHQSecured;

import java.util.List;
import java.util.Optional;

@Controller("/api/{cluster}/cluster-ops")
public class ClusterOpsController extends AbstractController {
    @Inject
    private ClusterOpsService clusterOpsService;

    @AKHQSecured(resource = Role.Resource.TRANSACTION, action = Role.Action.READ)
    @Get("/transactions")
    @Operation(tags = {"cluster ops"}, summary = "List in-flight and recent transactions on a cluster")
    public List<TransactionSummary> listTransactions(String cluster) throws Exception {
        checkIfClusterAllowed(cluster);
        return clusterOpsService.listTransactions(cluster);
    }

    @AKHQSecured(resource = Role.Resource.TRANSACTION, action = Role.Action.READ)
    @Get("/transactions/{transactionalId}")
    @Operation(tags = {"cluster ops"}, summary = "Describe one transaction")
    public TransactionDetail describeTransaction(String cluster, String transactionalId) throws Exception {
        checkIfClusterAllowed(cluster);
        return clusterOpsService.describeTransaction(cluster, transactionalId);
    }

    @AKHQSecured(resource = Role.Resource.TRANSACTION, action = Role.Action.DELETE)
    @Post(value = "/transactions/abort", consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"cluster ops"}, summary = "Abort a hung transaction so its consumers stop waiting on it")
    public HttpResponse<?> abortTransaction(String cluster, @Body @Valid AbortTransactionRequest body) throws Exception {
        checkIfClusterAllowed(cluster);
        clusterOpsService.abortTransaction(cluster, body);
        return HttpResponse.noContent();
    }

    @AKHQSecured(resource = Role.Resource.NODE, action = Role.Action.ALTER_CONFIG)
    @Post("/topic/{topic}/partition/{partition}/elect-preferred-leader")
    @Operation(tags = {"cluster ops"}, summary = "Trigger preferred leader election for one partition")
    public HttpResponse<?> electPreferredLeader(String cluster, String topic, int partition) throws Exception {
        checkIfClusterAllowed(cluster);
        clusterOpsService.electPreferredLeader(cluster, topic, partition);
        return HttpResponse.noContent();
    }

    @AKHQSecured(resource = Role.Resource.NODE, action = Role.Action.READ)
    @Get("/log-dirs")
    @Operation(tags = {"cluster ops"}, summary = "Describe log directories, sizes and usable space per broker")
    public List<LogDirEntry> describeLogDirs(String cluster, Optional<List<Integer>> brokerId) throws Exception {
        checkIfClusterAllowed(cluster);
        return clusterOpsService.describeLogDirs(cluster, brokerId.orElse(null));
    }

    @AKHQSecured(resource = Role.Resource.NODE, action = Role.Action.READ)
    @Get("/quorum")
    @Operation(tags = {"cluster ops"}, summary = "Describe the KRaft metadata quorum status")
    public QuorumView describeMetadataQuorum(String cluster) throws Exception {
        checkIfClusterAllowed(cluster);
        return clusterOpsService.describeMetadataQuorum(cluster);
    }
}
