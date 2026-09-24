# Contributing to ARK

## Before you start

Read [PLAN.md](PLAN.md) first. It's the source of truth for what ARK
is trying to be, what's deliberately out of scope, and why. Several
design decisions there (no general DAG engine, Kafka-coordinator
clustering instead of Raft, no new storage layer) were made after
weighing real tradeoffs. If a change would cross one of those lines,
open an issue to discuss it before writing code.

## Development setup

```
cd bridge-engine
go build ./...
go vet ./...
go test ./...
```

Run the full stack locally:

```
docker compose up
```

This starts a single-node Kafka broker, the bridge engine wired to
`bridge-engine/config.demo.yaml`, and `demo-echo`, a small callback target
that echoes what it receives (400 for `"invalid": true`, 500 for
`"fail": true`). The README's Quick start walks through it. To run your
own config, set `ARK_CONFIG=path/to/config.yaml`.

## Making a change

1. Fork the repo and create a branch off `main`.
2. Keep the change scoped to one thing: a bug fix, a config field, a
   feature. Large refactors or scope changes (see "before you start")
   go through an issue first.
3. `go build ./...`, `go vet ./...`, and `go test ./...` must pass. CI
   (`.github/workflows/ci.yml`) runs these plus `govulncheck`, `gosec`,
   Semgrep, OSV-Scanner, Gitleaks, and a Trivy scan of the built image
   on every PR, so it's worth running the fast ones locally first.
4. If you touched runtime behavior (not just docs), verify it against
   a real `docker compose up` stack, not just a successful build.
   Several bugs in this codebase, including one CI can't catch (a
   consumer-group race that only shows up against a genuinely fresh
   topic), were only caught by actually running the flow end to end
   (see PLAN.md's Phase 2 and Phase 4 notes for examples). Add a unit
   test for the logic you touched where one's practical; the
   `internal/config`, `internal/breaker`, `internal/rules`, and
   `internal/callback` packages have runnable examples of what "good"
   looks like here.
5. Open a pull request against `main` with a clear description of what
   changed and why.

## Code style

- No comments explaining *what* code does; clear names should cover
  that. A comment is fine only for a genuinely non-obvious *why*, a
  workaround, or a subtle invariant.
- Keep changes minimal, and don't refactor unrelated code in the same PR.
- Match the existing package structure (`internal/config`,
  `internal/consumer`, `internal/producer`, etc.) rather than
  introducing new top-level packages for small additions.

## Reporting bugs or proposing features

Open a GitHub issue. For a bug, include your pipeline config (redact
anything sensitive) and what you expected vs. what happened. For a
feature, check PLAN.md first. It may already be designed and staged
for later, or explicitly ruled out with the reasoning written down.
