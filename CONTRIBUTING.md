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
```

Run the full stack locally:

```
docker compose up
```

This starts a single-node Kafka broker and the bridge engine wired to
`bridge-engine/config.example.yaml`. There's no downstream app in the
base compose file, so bring your own, or point `target.url` at
something like [`hashicorp/http-echo`](https://github.com/hashicorp/http-echo)
for quick manual testing.

## Making a change

1. Fork the repo and create a branch off `main`.
2. Keep the change scoped to one thing: a bug fix, a config field, a
   feature. Large refactors or scope changes (see "before you start")
   go through an issue first.
3. `go build ./...` and `go vet ./...` must pass. There's no CI yet, so
   this is on you until Phase 5/6 tooling lands.
4. If you touched runtime behavior (not just docs), verify it against
   a real `docker compose up` stack, not just a successful build.
   Several bugs in this codebase were only caught by actually running
   the flow end to end (see PLAN.md's Phase 2 notes for an example).
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
