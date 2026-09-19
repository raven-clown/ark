package org.akhq.employee.alerting;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties("akhq.alerting")
@Data
public class AlertingProperties {
    private boolean enabled = false;
    private long lagThreshold = 10000;
    private String checkInterval = "5m";
}
