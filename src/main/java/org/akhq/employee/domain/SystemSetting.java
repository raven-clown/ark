package org.akhq.employee.domain;

import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.MappedProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@MappedEntity("system_settings")
@Data
@NoArgsConstructor
public class SystemSetting {
    @Id
    @MappedProperty("setting_key")
    private String settingKey;

    @MappedProperty("setting_value")
    private String settingValue;

    private String updatedBy;

    @DateUpdated
    private Instant updatedAt;

    public SystemSetting(String settingKey, String settingValue, String updatedBy) {
        this.settingKey = settingKey;
        this.settingValue = settingValue;
        this.updatedBy = updatedBy;
    }
}
