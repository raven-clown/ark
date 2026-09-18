create table cluster_connection_definitions (
    id bigint auto_increment primary key,
    name varchar(128) not null unique,
    bootstrap_servers varchar(1024) not null,
    schema_registry_url varchar(512),
    schema_registry_type varchar(32) not null default 'CONFLUENT',
    connects_json varchar(2048) not null default '[]',
    ksqldbs_json varchar(2048) not null default '[]',
    created_by varchar(64) not null,
    created_at timestamp not null,
    updated_at timestamp not null
);
