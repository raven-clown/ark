package org.akhq.employee.repository;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;
import org.akhq.employee.domain.SystemSetting;

import java.util.List;

@JdbcRepository(dialect = Dialect.H2)
public interface SystemSettingRepository extends CrudRepository<SystemSetting, String> {
    @Query("select * from system_settings order by setting_key asc")
    List<SystemSetting> findAllByOrderBySettingKeyAsc();
}
