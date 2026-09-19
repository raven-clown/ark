package org.akhq.employee.alerting;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URL;
import java.util.Map;

@Slf4j
@Singleton
public class WebhookSender {
    public void send(TeamWebhook webhook, String message) {
        try {
            switch (webhook.getWebhookType()) {
                case SLACK -> sendSlack(webhook, message);
                case LINE -> sendLine(webhook, message);
                default -> sendGeneric(webhook, message);
            }
        } catch (Exception e) {
            log.warn("Failed to deliver alert webhook to team {}", webhook.getTeamId(), e);
        }
    }

    private void sendSlack(TeamWebhook webhook, String message) throws Exception {
        post(webhook.getWebhookUrl(), req -> req.contentType(MediaType.APPLICATION_JSON).body(Map.of("text", message)));
    }

    private void sendLine(TeamWebhook webhook, String message) throws Exception {
        post("https://notify-api.line.me/api/notify", req -> req
            .header("Authorization", "Bearer " + webhook.getLineToken())
            .contentType(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
            .body("message=" + message));
    }

    private void sendGeneric(TeamWebhook webhook, String message) throws Exception {
        post(webhook.getWebhookUrl(), req -> req.contentType(MediaType.APPLICATION_JSON).body(Map.of("message", message)));
    }

    private void post(String targetUrl, java.util.function.UnaryOperator<MutableHttpRequest<Object>> customize) throws Exception {
        URL url = URI.create(targetUrl).toURL();
        URL originOnly = new URL(url.getProtocol(), url.getHost(), url.getPort(), "");
        String pathAndQuery = url.getFile();

        try (HttpClient client = HttpClient.create(originOnly)) {
            MutableHttpRequest<Object> request = HttpRequest.POST(pathAndQuery, null);
            client.toBlocking().exchange(customize.apply(request));
        }
    }
}
