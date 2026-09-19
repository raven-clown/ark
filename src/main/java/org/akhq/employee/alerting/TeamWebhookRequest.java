package org.akhq.employee.alerting;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Introspected
public class TeamWebhookRequest {
    @NotNull
    public WebhookType webhookType;

    @NotBlank
    public String webhookUrl;

    public String lineToken;
}
