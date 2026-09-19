package org.akhq.employee.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.akhq.employee.alerting.TeamWebhook;
import org.akhq.employee.alerting.TeamWebhookRepository;
import org.akhq.employee.alerting.TeamWebhookRequest;
import org.akhq.employee.repository.TeamRepository;

import java.util.List;
import java.util.NoSuchElementException;

@Secured("ROLE_ADMIN")
@Controller("/api/admin/teams/{teamName}/webhooks")
public class AdminTeamWebhookController {
    @Inject
    private TeamRepository teamRepository;

    @Inject
    private TeamWebhookRepository teamWebhookRepository;

    @Error(exception = NoSuchElementException.class)
    public HttpResponse<?> notFound(NoSuchElementException e) {
        return HttpResponse.notFound(e.getMessage());
    }

    @Get
    @Operation(tags = {"admin"}, summary = "List a team's alert webhooks")
    public List<TeamWebhook> list(String teamName) {
        return teamWebhookRepository.findByTeamId(requireTeamId(teamName));
    }

    @Post(consumes = MediaType.APPLICATION_JSON)
    @Operation(tags = {"admin"}, summary = "Add an alert webhook for a team")
    public TeamWebhook add(String teamName, @Body @Valid TeamWebhookRequest body, Authentication authentication) {
        Long teamId = requireTeamId(teamName);
        String createdBy = authentication != null ? authentication.getName() : "unknown";
        return teamWebhookRepository.save(
            new TeamWebhook(teamId, body.webhookType, body.webhookUrl, body.lineToken, createdBy));
    }

    @Delete("/{id}")
    @Operation(tags = {"admin"}, summary = "Remove a team's alert webhook")
    public HttpResponse<?> remove(String teamName, Long id) {
        Long teamId = requireTeamId(teamName);
        teamWebhookRepository.deleteByIdAndTeamId(id, teamId);
        return HttpResponse.noContent();
    }

    private Long requireTeamId(String teamName) {
        return teamRepository.findByName(teamName)
            .orElseThrow(() -> new NoSuchElementException("Team not found: " + teamName))
            .getId();
    }
}
