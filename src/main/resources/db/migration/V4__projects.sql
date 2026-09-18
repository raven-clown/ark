create table projects (
    id bigint auto_increment primary key,
    name varchar(128) not null,
    slug varchar(64) not null unique,
    description varchar(512),
    created_by varchar(64) not null,
    created_at timestamp not null,
    updated_at timestamp not null
);

create table project_members (
    id bigint auto_increment primary key,
    project_id bigint not null references projects(id) on delete cascade,
    employee_id bigint not null references employees(id) on delete cascade,
    project_role varchar(16) not null,
    added_by varchar(64) not null,
    added_at timestamp not null,
    constraint uq_project_member unique (project_id, employee_id)
);

create table project_clusters (
    id bigint auto_increment primary key,
    project_id bigint not null references projects(id) on delete cascade,
    cluster_name varchar(128) not null,
    added_by varchar(64) not null,
    added_at timestamp not null,
    constraint uq_project_cluster unique (project_id, cluster_name)
);

create index idx_project_members_employee on project_members(employee_id);
create index idx_project_clusters_project on project_clusters(project_id);
