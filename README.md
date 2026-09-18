# Ark

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)

Kafka GUI for [Apache Kafka](http://kafka.apache.org/) to manage topics, topic data, consumer
groups, schema registry, Kafka Connect, ksqlDB and more, extended with employee identity, team
based access control, self-service projects and an audit trail built for on-premise, enterprise
use. Built on top of AKHQ.

## What Ark adds on top of AKHQ

* **Employee login**: sign in with an employee code looked up against a central company API, or
  with a manually created account and password.
* **Central admin dashboard**: platform admins see every employee pulled in from the directory
  and grant access per team or per person, down to resource and action (read a topic, produce to
  it, manage a connector, and so on).
* **Two tier admin hierarchy**: a regular admin can configure everything day to day, only a super
  admin can change another super admin's account.
* **Projects**: any employee can create a project, become its owner, add teammates with a role
  (viewer, developer, maintainer, owner) and link the Kafka clusters the project actually uses.
  Access is logical only, every project shares the same underlying clusters.
* **Audit log viewer**: search and filter the existing `akhq-audit` topic by employee, team, or
  action type, with a print view for compliance reporting.
* **Cluster connection definitions**: define a cluster's bootstrap servers, schema registry and
  connect endpoints through a form, and get back the exact YAML block to place in
  `application.yml`.
* **Retention settings**: default data retention periods editable from the admin dashboard.

## Core Kafka features (inherited from AKHQ)

* Topics: browse, search, create, configure, tail and produce data
* Consumer groups: view lag, members, reset or delete offsets
* Schema registry, Kafka Connect and ksqlDB browsing and management
* Access control list viewer
* Node and broker inspection

## Getting started

Backend, requires JDK 25:

```bash
./gradlew run
```

Frontend, for local development:

```bash
cd client
npm install
npm run start
```

Application configuration lives in `src/main/resources/application.yml`. The
`akhq.employee-directory` section controls the employee login flow, including the mock directory
client used for local development.

## Documentation

* Architecture and RBAC data model: `docs/enterprise/architecture-blueprint.md`
* Draft blueprint for a fuller multi-tenant, multi-cluster control plane, not implemented, kept
  as reference: `docs/enterprise/multi-tenant-control-plane-blueprint.md`

## Security

Read the "Before deploying this anywhere reachable by untrusted users" section of
`docs/enterprise/architecture-blueprint.md` before any real deployment. The employee login flow
authenticates on employee code alone by design; that document lays out the tradeoffs and the
options for closing the gap.

## License

Apache License 2.0. See [LICENSE](LICENSE).
