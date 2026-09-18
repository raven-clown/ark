alter table employees add column is_super_admin boolean not null default false;

create table system_settings (
    setting_key varchar(128) primary key,
    setting_value varchar(1024) not null,
    updated_by varchar(64) not null,
    updated_at timestamp not null
);

insert into system_settings (setting_key, setting_value, updated_by, updated_at) values
    ('audit_log_retention_days', '365', 'system', current_timestamp),
    ('access_log_retention_days', '180', 'system', current_timestamp),
    ('rbac_change_history_retention_days', '2555', 'system', current_timestamp);
