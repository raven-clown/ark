package org.akhq.employee.service;

import jakarta.inject.Singleton;
import org.akhq.employee.domain.SystemSetting;
import org.akhq.employee.repository.SystemSettingRepository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Retention defaults reflect common ISO 27001 / SOC2 audit evidence practice: at least one
 * full audit cycle (365 days) for audit logs, and long-term retention for RBAC change history
 * (7 years, 2555 days) so who-granted-what-when can be reconstructed during a compliance review.
 * Every value is editable by any admin at runtime, defaults only apply on first install.
 */
@Singleton
public class RetentionSettingsService {
    public static final String AUDIT_LOG_RETENTION_DAYS = "audit_log_retention_days";
    public static final String ACCESS_LOG_RETENTION_DAYS = "access_log_retention_days";
    public static final String RBAC_CHANGE_HISTORY_RETENTION_DAYS = "rbac_change_history_retention_days";

    private final SystemSettingRepository repository;

    public RetentionSettingsService(SystemSettingRepository repository) {
        this.repository = repository;
    }

    public Map<String, String> listAll() {
        return repository.findAllByOrderBySettingKeyAsc().stream()
            .collect(Collectors.toMap(SystemSetting::getSettingKey, SystemSetting::getSettingValue));
    }

    public int getRetentionDays(String key, int fallback) {
        return repository.findById(key)
            .map(s -> Integer.parseInt(s.getSettingValue()))
            .orElse(fallback);
    }

    public void update(String key, String value, String updatedBy) {
        boolean exists = repository.existsById(key);
        SystemSetting setting = new SystemSetting(key, value, updatedBy);
        if (exists) {
            repository.update(setting);
        } else {
            repository.save(setting);
        }
    }

    public List<SystemSetting> listRaw() {
        return repository.findAllByOrderBySettingKeyAsc();
    }
}
