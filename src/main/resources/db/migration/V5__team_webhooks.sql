create table team_webhooks (
    id bigint auto_increment primary key,
    team_id bigint not null references teams(id) on delete cascade,
    webhook_type varchar(16) not null default 'GENERIC',
    webhook_url varchar(512) not null,
    line_token varchar(255),
    is_enabled boolean not null default true,
    created_by varchar(64) not null,
    created_at timestamp not null,
    constraint uq_team_webhook unique (team_id, webhook_url)
);
