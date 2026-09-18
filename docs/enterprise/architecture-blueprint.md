# Enterprise Kafka Control Platform - Architecture Blueprint

Base: AKHQ 0.28.0. Backend is Micronaut (unchanged). Frontend turned out to be React 19 + Vite +
Bootstrap 5, not Vue.js as first assumed - confirmed by reading `client/package.json` before any
frontend work started, and the new admin screens were built in React to match, rather than
standing up a second framework alongside the existing one. Everything new lives under
`org.akhq.employee.*` on the backend and `client/src/containers/Admin/` on the frontend, so the
original AKHQ code stays untouched and upgradeable.

## Before deploying this anywhere reachable by untrusted users: read this

The employee login flow authenticates on **employee code alone** - the password field submitted
by the login form is never checked (`EmployeeAuthenticationProvider` never reads
`authenticationRequest.getSecret()`). This was built to match the brief exactly ("กรอกรหัสพนักงาน
... ยิง API ไปยังระบบกลาง... ดึงรหัสพนักงานและชื่อ-นามสกุล" - no password in that flow at all), but
a two-agent security review independently flagged the consequence as critical and it needs a
product decision, not a silent code patch:

- With the default `akhq.employee-directory.mock-enabled: true`, **any non-blank string** typed
  into the login form authenticates as a new employee. This is now fail-closed specifically
  against the worst case - `EmployeeAuthenticationService` refuses to grant bootstrap
  admin/super-admin while mock mode is active, and `EmployeeAuthSecurityWarning` logs a loud
  startup warning whenever mock mode is on with `micronaut.security.enabled: true` - but the base
  "any string logs in as *some* employee" behavior is inherent to mock mode, by design, for local
  development.
- Even with the real directory (`mock-enabled: false`), this flow authenticates on a value that is
  fundamentally a **username, not a secret** - anyone who knows or guesses a valid employee code
  (sequential IDs, a leaked HR list, an existing admin's own code) can log in as that person,
  admins included. Combined with the login form being a plain cookie-based POST with no CSRF
  token, a malicious page can also force a victim's browser to authenticate as an
  attacker-chosen identity (classic login-CSRF), which matters far more here than in AKHQ's
  original basic-auth login because no valid password is needed at all.

**This is not something to fix by inventing a password requirement that contradicts the brief.**
The three real options, in order of how much they change:
1. Deploy this only behind a boundary that already authenticates the caller - VPN, an internal-only
   network, or a reverse proxy doing real SSO - so the employee-code lookup is a profile-completion
   step after real authentication, not the authentication itself.
2. Confirm the real corporate directory API (once `mock-enabled: false`) is not a bare lookup but
   itself performs a genuine credential or OTP check before returning a match - if so, the risk
   above is mostly theoretical against that specific deployment.
3. Add a real second factor in this app (password, OTP, magic link) on top of the directory lookup,
   which is a genuine feature addition, not a fix, and changes the login UX described in the brief.

Pick one of these before this is reachable by anyone who isn't already trusted. Nothing in Phase 1
below assumes this is resolved.

## Phase 1 - Employee identity and RBAC (implemented in this pass)

### Login flow
1. Employee enters an employee code in the login screen.
2. `EmployeeAuthenticationProvider` calls `EmployeeDirectoryClient.lookup(code)`.
   - `MockEmployeeDirectoryClient` is active by default (`akhq.employee-directory.mock-enabled: true`)
     so the whole flow runs without a real corporate endpoint during development.
   - `HttpEmployeeDirectoryClient` calls the real central company API when mock is turned off;
     base URL, lookup path and API key header are all config-driven
     (`akhq.employee-directory.*` in [application.yml](../../src/main/resources/application.yml)).
   - Only employee code and full name are pulled back from the central system. Nothing else.
3. `EmployeeAuthenticationService` upserts an `employees` row (create on first login, refresh the
   name on every login after).
4. Login goes through Micronaut Security's existing `/login` endpoint (already wired for AKHQ's
   basic-auth users) - POST `{"username": "<employeeCode>", "password": "<anything>"}`.
   `EmployeeAuthenticationProvider` ignores the password field entirely; it is only present
   because the endpoint's request shape expects one.
5. A manual, password-based account type also exists (`ManualAccountAuthenticationProvider`) for
   people who are not in the corporate directory at all - created by an admin with
   `POST /api/admin/employees/manual`, authenticated the normal way through the same `/login`
   endpoint with a real password (bcrypt via `jbcrypt`, already an AKHQ dependency).

### RBAC data model
```
employees            one row per person, however they log in
  employee_code, full_name, is_admin, is_super_admin, is_active,
  account_type (EMPLOYEE_DIRECTORY | MANUAL), password_hash, last_login_at

teams                DEVOPS / QA / MQA / DATA_ENGINEER / OPERATOR, seeded, admin can add more
employee_teams       many-to-many

permission_grants        per-employee grant: resource + action + cluster_pattern + topic_pattern
team_permission_grants   same shape, applied to every member of a team

system_settings          key/value, retention policy lives here
```
`resource`/`action` reuse the exact enums AKHQ already defines in
[Role.java](../../src/main/java/org/akhq/configs/security/Role.java) (`TOPIC`, `CONNECTOR`,
`SCHEMA`, ... × `READ`, `CREATE`, `UPDATE_STATE`, ...), so the new DB-driven grants speak the same
vocabulary as AKHQ's original YAML-driven roles.

An employee's **effective permissions** = their individual grants ∪ every grant attached to every
team they belong to (`PermissionResolutionService`). This gives the admin two ways to assign
access, both explicitly requested: per-team defaults and per-person overrides.

### Enforcement
- `EmployeeGrantSecurityRule` is a second Micronaut `SecurityRule` sitting next to AKHQ's existing
  `AKHQSecurityRule`. It only activates for authentications tagged
  `auth_source = EMPLOYEE_DIRECTORY`; every other login method (LDAP/OIDC/GitHub/basic-auth YAML)
  still goes through the original rule, untouched. It resolves grants live from the database on
  every request rather than baking them into the JWT, so an admin revoking access takes effect
  immediately without waiting for the token to expire.
- **Fixed, not just designed:** AKHQ also does a second, finer check inside individual controllers
  - `AbstractController.checkIfClusterAndResourceAllowed(cluster, resourceName)` - which matches
  the actual topic/group name against `Group.patterns`. Initially this stayed on the legacy YAML
  path for employee logins, which would have made every employee's cluster list come back empty
  (stuck on a blank screen) and thrown on every `@AKHQSecured` route, because `AKHQSecurityRule`/
  `AbstractController.getUserGroups()` call `unrollGroups()`, which expects a `"groups"` JWT claim
  employee logins never set. `AbstractController.getUserGroups()`,
  `buildUserBasedResourceFilters()`, and `checkIfClusterAndResourceAllowed()` now all branch on
  `isEmployeeAuthentication()` and resolve grants live via `PermissionResolutionService` instead;
  `AKHQSecurityRule.check()` defers (`UNKNOWN`) for employee-sourced auth so it never reaches the
  YAML path that would throw. `AkhqController.auths()`/`getRights()` were patched the same way so
  `/api/me` and the login form itself work for employees. See
  [AbstractController.java](../../src/main/java/org/akhq/controllers/AbstractController.java).

### Security review findings (fixed before this pass was considered done)
Two independent focused reviews (guidance-mode use of Cloudflare's `security-audit-skill`
checklists) were run against this module. Confirmed findings and their fixes:
- **Critical - `POST /api/admin/rbac/import` bypassed the admin hierarchy guard.** A plain admin
  could import a record granting themselves `superAdmin: true`, or demote/deactivate an existing
  super admin, none of which `setAdmin`/`setSuperAdmin`/`setActive` would have allowed directly.
  Fixed in `RoleAssignmentService.importData()`: every record is validated against the acting
  admin's own super-admin status before any write happens.
- **High - bootstrap admin codes re-escalated on every login.** `EmployeeAuthenticationService`
  re-applied `bootstrap-admin-codes` on every login, not just account creation, silently undoing a
  deliberate later demotion. Fixed: bootstrap status now only ever applies once, when the account
  is first created.
- **High - `/api/admin/**` trusted the JWT's role claim, not live DB state.** A demoted or
  deactivated admin kept API access until their token expired. Fixed by adding
  `AdminAccessSecurityRule`, which re-checks `is_admin`/`is_active` from the database on every
  request under `/api/admin/**`, mirroring how `EmployeeGrantSecurityRule` already does this for
  `@AKHQSecured` routes.
- **Critical - passwordless login + mock directory = anyone becomes super admin.** See the
  "Before deploying this anywhere" section at the top of this document. Partially mitigated
  (bootstrap seeding now refuses to run while mock mode is active, and a startup warning fires),
  but the underlying design question needs a decision, not a patch.
- **Medium - mock directory client was silently enabled by default.** Fixed with
  `EmployeeAuthSecurityWarning`, a `@Context`/`@PostConstruct` bean matching AKHQ's own existing
  `JwtSecurityWarning` pattern, which logs a loud startup warning when mock mode is on.
- Checked and found fine by both reviews: SQL injection (Micronaut Data's bound `@Query`
  parameters throughout), password hashing (bcrypt via jbcrypt, correct API usage), XSS in the new
  React admin screens (no `dangerouslySetInnerHTML`, all values render as plain JSX text), open
  redirects, CORS (untouched by this work), and privilege-escalation guardrails on the direct
  `setSuperAdmin` API path (already required a live super-admin check before this review).

### Admin hierarchy
Two levels, both able to configure every part of the system day to day (teams, grants, retention
settings, cluster connections once Phase 3 lands):
- **Admin** (`is_admin`): full dashboard access.
- **Super admin** (`is_super_admin`): same as admin, plus the only one who can touch another
  super admin's account (promote, demote, deactivate). A regular admin calling those endpoints
  against a super-admin target gets `403` (`AdminHierarchyException`).
- Bootstrap: `akhq.employee-directory.bootstrap-admin-codes` in config lists employee codes that
  become super admin automatically on first login - solves the chicken-and-egg problem of needing
  an admin before anyone can grant admin.

### Retention / audit defaults
`system_settings` seeds three values on install, editable by any admin afterward
(`GET/PATCH /api/admin/settings/retention`):

| Key | Default | Basis |
|---|---|---|
| `audit_log_retention_days` | 365 | one full audit cycle - common SOC2/ISO 27001 evidence window |
| `access_log_retention_days` | 180 | operational access logs, shorter-lived than audit evidence |
| `rbac_change_history_retention_days` | 2555 (~7 years) | who-granted-what-when, kept long enough to survive a multi-year compliance review |

These are defaults, not hard limits - the numbers exist so day one has a defensible policy instead
of "keep forever" or "keep nothing."

### Backup / portability
`GET /api/admin/rbac/export` and `POST /api/admin/rbac/import` round-trip every employee, team and
grant as one JSON document - for moving between environments, or as a config backup alongside
whatever volume the deployment already mounts for Kafka data (see Deployment below).

### Files
```
src/main/java/org/akhq/employee/
  domain/         Employee, Team, EmployeeTeamMembership, PermissionGrant, TeamPermissionGrant,
                  SystemSetting, AccountType
  repository/     Micronaut Data JDBC repositories for the above
  directory/      EmployeeDirectoryClient (interface), Mock + Http implementations
  config/         EmployeeDirectoryProperties, EmployeeAuthSecurityWarning
  service/        EmployeeAuthenticationService, RoleAssignmentService, ManualAccountService,
                  PermissionResolutionService, RetentionSettingsService, AdminHierarchyException
  security/       EmployeeAuthenticationProvider, ManualAccountAuthenticationProvider,
                  EmployeeGrantSecurityRule, AdminAccessSecurityRule
  controller/     AdminEmployeeController, AdminAuditController, AdminClusterConnectionController
  dto/            request/response shapes
  audit/          AuditLogService, AuditLogEntry, AuditLogQuery (reads akhq-audit back out)
  cluster/        ClusterConnectionDefinition/Service (definitions + YAML generator, see Phase 3)
src/main/resources/db/migration/  Flyway migrations (H2 by default, file-backed under ./data)
client/src/containers/Admin/       AdminEmployeeList, AdminEmployeeDetail, AdminAuditLog,
                                    AdminClusterConnections (React, Bootstrap 5)
```

Storage: H2 file database at `${AKHQ_DATA_DIR:./data}/akhq-rbac` - one env var to point at a
mounted volume in Docker, same idea as NiFi's `nifi.properties` repository paths. Swapping to
Postgres for a multi-instance deployment later means changing the datasource URL/driver and the
`@JdbcRepository(dialect = ...)` value on each repository; the SQL itself is plain ANSI and needs
no rewrite.

## Phase 2 - UI/UX (basic admin screens shipped this pass; the design-system ask is not started)

The frontend turned out to be React 19 + Vite + Bootstrap 5 (confirmed by reading
`client/package.json`), not Vue.js - the new admin screens (`client/src/containers/Admin/`) were
built to match rather than introducing a second framework. They reuse Bootstrap's existing form
controls (`<select>`, `<input>`), not the fully custom design system the brief asks for.

- English-language UI, light/dark theme (not literal black-and-white - a proper dark mode toggle
  alongside the default light theme) - not started; the current admin screens inherit whatever
  theme the existing AKHQ shell uses.
- Every interactive control (select, dropdown, date picker, hover/focus states) built as a custom
  component rather than a stock UI kit's default look - a small internal design system, not
  Bootstrap's out-of-the-box styling. This is real, deliberately deferred work: the admin screens
  shipped this pass use plain Bootstrap controls so the RBAC feature had something usable behind
  it tonight, not because the custom-design-system ask was dropped.
- Fast, animated transitions throughout - this is a navigation-heavy admin tool, so perceived
  speed matters as much as raw load time.
- Stream Governance / pipeline view: a Vue Flow canvas showing source → topic → sink, animated
  edges reflecting live throughput/lag rather than a static diagram - closer to watching data move
  than reading a topology snapshot. This is the single most-repeated ask across the brief, so it
  should be the first Phase 2 milestone once the RBAC backend has a UI to sit behind.
- No-code Kafka Connect wizard: schema/table picked from dropdowns sourced from the schema
  registry, a toggle for `snapshot.mode=no_data` vs incremental snapshot with a configurable chunk
  size.
- Every admin-configurable value (retention days, employee-directory endpoint, bootstrap admins)
  gets a settings screen - the backend already exposes it as JSON, this phase is wiring a form to
  it, not inventing new config surface.

## Phase 3 - Ops: dynamic clusters, audit UI, alerting

### Audit log viewer - done
`AuditLogService` (`org.akhq.employee.audit`) reads AKHQ's own `akhq-audit` topic back out via
`KafkaModule.getConsumer()` - the same consumer-creation path `RecordRepository` already uses, so
nothing new was added to the Kafka client layer. `GET /api/admin/audit-log` filters by employee
code, team (resolved through the Phase 1 RBAC tables), action type, and a time range; the scan is
bounded on both records inspected (20,000) and wall-clock polls (40 × 500ms) so a broad or
empty-result query can't turn into an unbounded read against a live topic. The admin screen at
`/ui/admin/audit-log` has a filter form, a results table, and a print button (`window.print()`) for
"print a report for review," matching the brief. Not yet done: CSV/PDF export beyond browser print,
and pagination past the 500-row cap for a single query (the time-range filter is the workaround for
now - narrow the window and re-search).

### Dynamic cluster management - definitions + YAML generator done; hot-reload deliberately not attempted
Cluster connections still actually load from `akhq.connections` in YAML at boot
([Connection.java](../../src/main/java/org/akhq/configs/Connection.java)) - that part is
unchanged. What's new: `org.akhq.employee.cluster` (`ClusterConnectionDefinition`,
`ClusterConnectionService`, `AdminClusterConnectionController` at `/api/admin/cluster-connections`,
and `/ui/admin/cluster-connections`) lets an admin define a cluster (name, bootstrap servers,
optional schema registry, connect/ksqlDB endpoints) through a validated form instead of hand-editing
YAML, and generates the exact `akhq.connections.<name>` YAML block to paste into `application.yml`.
Saving a definition here does **not** make the cluster reachable yet - the UI says so explicitly,
and the generated YAML still has to be pasted in and the app restarted.

A focused investigation of `KafkaModule.java` (the single chokepoint every repository and
controller goes through to reach a `Connection` - confirmed by reading every reference) found that
true hot-reload is architecturally contained, not a rewrite: replace the injected
`List<Connection> connections` field in `KafkaModule` with a mutable registry, add
`addConnection()`/`removeConnection()`, and evict the cached
`AdminClient`/`KafkaProducer`/`SchemaRegistryClient`/etc. entries for a removed cluster (none of
that eviction exists today, even for the static case - a pre-existing gap worth fixing alongside
this). `AkhqController.java:39`'s direct `List<Connection>` injection is the only other site that
would need to switch to reading through `KafkaModule` instead of its own copy.

That wiring was deliberately **not implemented tonight**: it is the one remaining item that would
touch core, shared Kafka connectivity used by every existing AKHQ feature (topic browsing,
consumer groups, schema registry, connect, ksqlDB) rather than being purely additive, and there is
no real Kafka broker available in this environment to test the change end-to-end against. Building
it blind, unreviewed, and unable to verify it doesn't break existing cluster connectivity was
judged too risky to do unsupervised overnight, even though the investigation says it's a
well-scoped change. Do that wiring next, against a real cluster.

### Alerting - not started
A Micronaut `@Scheduled` job polling consumer-group lag and connector status, firing to per-team
Slack/Line webhooks - webhook URLs are a natural fit for the `system_settings` table or a small
dedicated `team_webhooks` table next to it.

## Phase 4 - MCP server / AI copilot (not started)

- A Micronaut-hosted MCP server module exposing tools (schema inspection, lag lookup,
  troubleshooting FAILED connectors, config generation) that call the exact same
  `PermissionResolutionService` used by `EmployeeGrantSecurityRule` - the AI never gets a
  privilege the calling employee doesn't already have. This is the reason Phase 1's authorization
  is a service (`PermissionResolutionService`) rather than logic embedded in the security rule: an
  MCP tool handler and an HTTP controller can both call it the same way.

## Login/SSO roadmap (raised mid-build, scoped here rather than bolted on ad hoc)

AKHQ already ships, out of the box, independent of anything built in Phase 1:
- **OIDC/OAuth2** - `micronaut-security-oauth2` + `OidcUserDetailsMapper`, config-driven per
  provider (Azure AD, Okta, Keycloak, Google, anything OIDC-compliant - this covers most "AWS SSO"
  / ADFS-via-OIDC setups already).
- **LDAP / Active Directory** - `LdapContextAuthenticationMapper`.
- **GitHub OAuth** - `GithubAuthenticationMapper`.

Not currently in AKHQ or this fork, and each a distinct piece of work:
- **SAML 2.0** - Micronaut Security has no first-party SAML module; would need a library
  (`pac4j-saml` is the usual choice) wired in as a new authentication provider, following the same
  pattern as `EmployeeAuthenticationProvider`. Worth doing once there's a specific IdP to test
  against, since SAML implementations vary enough between vendors (ADFS vs Okta vs Azure AD) that
  it's not safely built against no real endpoint.
- **TLS everywhere** - Micronaut server TLS is a config block (`micronaut.server.ssl`), not new
  code; belongs in the deployment/Docker pass, not the auth module.
- **SFTP** - not an auth concern as such; if this refers to shipping config exports or audit log
  archives over SFTP, that is a small scheduled job once Phase 3's audit storage exists, not
  something to guess the shape of now.

Manual account + password creation with a super-admin gate is implemented in Phase 1 already
(`ManualAccountService`, `POST /api/admin/employees/manual`) - that part of this ask is done, not
just roadmapped.

## What to do next

1. **Decide the employee-login security question at the top of this document first.** Everything
   else assumes this app sits behind a trusted boundary, a real credential-checking directory, or
   gets a second factor added - pick one before any real deployment.
2. Run the app once (`micronaut.security.enabled: true`, set at least one
   `akhq.employee-directory.bootstrap-admin-codes` entry) and exercise the login, admin dashboard,
   audit log, and cluster-connection screens by hand against a real Kafka cluster. Both the backend
   (`./gradlew compileJava`) and frontend (`npm run build` in `client/`) build clean, and the built
   frontend was loaded once in a real browser with no JS errors - but nothing in this pass has been
   exercised end-to-end against a live broker or actually clicked through by a person.
3. Do the `KafkaModule` hot-reload wiring for cluster connections (scoped precisely above) against
   a real cluster, now that the definitions/YAML-generator half exists.
4. Pick a Phase 2 starting point - the pipeline visualization is the most-requested single screen
   and the custom design system is genuinely unstarted (current admin screens use plain Bootstrap
   controls); both are reasonable places to begin.
