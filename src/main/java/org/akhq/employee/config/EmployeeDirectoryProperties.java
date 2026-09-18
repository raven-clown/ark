package org.akhq.employee.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

import java.util.List;

@ConfigurationProperties("akhq.employee-directory")
@Data
public class EmployeeDirectoryProperties {
    private boolean mockEnabled = true;
    private String baseUrl;
    private String lookupPath = "/employees/{employeeCode}";
    private String apiKeyHeader = "X-Api-Key";
    private String apiKey;
    private int timeoutSeconds = 5;
    private List<String> bootstrapAdminCodes = List.of();
}
