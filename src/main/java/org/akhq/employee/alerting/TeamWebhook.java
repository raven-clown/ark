package org.akhq.employee.alerting;

import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.annotation.TypeDef;
import io.micronaut.data.model.DataType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@MappedEntity("team_webhooks")
@Data
@NoArgsConstructor
public class TeamWebhook {
    @Id
    @GeneratedValue
    private Long id;

    private Long teamId;

    @TypeDef(type = DataType.STRING)
    private WebhookType webhookType = WebhookType.GENERIC;

    private String webhookUrl;
    private String lineToken;

    @MappedProperty("is_enabled")
    private boolean enabled = true;

    private String createdBy;

    @DateCreated
    private Instant createdAt;

    public TeamWebhook(Long teamId, WebhookType webhookType, String webhookUrl, String lineToken, String createdBy) {
        this.teamId = teamId;
        this.webhookType = webhookType;
        this.webhookUrl = webhookUrl;
        this.lineToken = lineToken;
        this.createdBy = createdBy;
    }
}
