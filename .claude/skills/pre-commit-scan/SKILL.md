---
name: pre-commit-scan
description: Run the same static analysis ARK's CI runs (gosec, go vet, go build, go test) before committing Go changes in bridge-engine. Use before any commit that touches bridge-engine/**/*.go.
---

# Pre-commit scan for bridge-engine

CI (`.github/workflows/ci.yml`) runs `gosec`, `semgrep`, `osv-scanner`,
`gitleaks`, `vulncheck`, `docker-build-scan`, and `build-test` on every push.
Reproduce the Go-side checks locally before committing so CI isn't the first
place a problem shows up:

```
cd bridge-engine
go build ./...
go vet ./...
go test ./...
go install github.com/securego/gosec/v2/cmd/gosec@latest   # first time only
$(go env GOPATH)/bin/gosec ./...
```

If gosec flags something real (e.g. an unsigned-to-signed integer
conversion that can actually overflow), fix the underlying logic, don't just
suppress it. Only add a `#nosec` comment when the value is provably bounded
before the conversion, and say why in the comment.

No AI mentions in code, comments, or commit messages. No Co-Authored-By
trailer. Comments only where the WHY isn't obvious from the code itself.
