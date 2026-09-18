package org.akhq.employee.directory;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.employee.config.EmployeeDirectoryProperties;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
@Requires(property = "akhq.employee-directory.mock-enabled", value = "false")
public class HttpEmployeeDirectoryClient implements EmployeeDirectoryClient {
    private final EmployeeDirectoryProperties properties;
    private final HttpClient httpClient;

    public HttpEmployeeDirectoryClient(EmployeeDirectoryProperties properties) {
        this.properties = properties;
        try {
            this.httpClient = HttpClient.create(URI.create(properties.getBaseUrl()).toURL());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                "akhq.employee-directory.base-url is not a valid URL: " + properties.getBaseUrl(), e);
        }
    }

    @Override
    public Optional<EmployeeDirectoryRecord> lookup(String employeeCode) {
        String path = properties.getLookupPath().replace("{employeeCode}", employeeCode);
        HttpRequest<?> request = HttpRequest.GET(path)
            .header(properties.getApiKeyHeader(), properties.getApiKey());

        try {
            HttpResponse<Map> response = httpClient.toBlocking().exchange(
                request, Map.class
            );
            Map<?, ?> body = response.body();
            if (body == null) {
                return Optional.empty();
            }
            String code = String.valueOf(body.get("employeeCode"));
            String fullName = String.valueOf(body.get("fullName"));
            return Optional.of(new EmployeeDirectoryRecord(code, fullName));
        } catch (HttpClientResponseException e) {
            if (e.getStatus() == HttpStatus.NOT_FOUND) {
                return Optional.empty();
            }
            log.warn("Employee directory lookup failed for {}: {}", employeeCode, e.getMessage());
            throw e;
        }
    }
}
