create table employees (
    id bigint auto_increment primary key,
    employee_code varchar(64) not null unique,
    full_name varchar(255) not null,
    is_admin boolean not null default false,
    is_active boolean not null default true,
    account_type varchar(32) not null default 'EMPLOYEE_DIRECTORY',
    password_hash varchar(255),
    created_at timestamp not null,
    updated_at timestamp not null,
    last_login_at timestamp
);

create table teams (
    id bigint auto_increment primary key,
    name varchar(64) not null unique,
    description varchar(255)
);

create table employee_teams (
    id bigint auto_increment primary key,
    employee_id bigint not null references employees(id) on delete cascade,
    team_id bigint not null references teams(id) on delete cascade,
    constraint uq_employee_team unique (employee_id, team_id)
);

create table permission_grants (
    id bigint auto_increment primary key,
    employee_id bigint not null references employees(id) on delete cascade,
    resource varchar(64) not null,
    action varchar(64) not null,
    cluster_pattern varchar(255) not null default '.*',
    topic_pattern varchar(255) not null default '.*',
    granted_by varchar(64) not null,
    created_at timestamp not null
);

create index idx_permission_grants_employee on permission_grants(employee_id);

create table team_permission_grants (
    id bigint auto_increment primary key,
    team_id bigint not null references teams(id) on delete cascade,
    resource varchar(64) not null,
    action varchar(64) not null,
    cluster_pattern varchar(255) not null default '.*',
    topic_pattern varchar(255) not null default '.*',
    granted_by varchar(64) not null,
    created_at timestamp not null
);

create index idx_team_permission_grants_team on team_permission_grants(team_id);

insert into teams (name, description) values
    ('DEVOPS', 'ทีม DevOps ดูแลระบบ pipeline และ infrastructure'),
    ('QA', 'ทีม QA ทดสอบคุณภาพระบบ'),
    ('MQA', 'ทีม MQA ตรวจสอบคุณภาพหน้างาน'),
    ('DATA_ENGINEER', 'ทีม Data Engineer ดูแลท่อข้อมูล'),
    ('OPERATOR', 'ทีม Operator ผู้ควบคุมหน้างาน');
