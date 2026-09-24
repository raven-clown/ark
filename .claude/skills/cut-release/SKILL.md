---
name: cut-release
description: Cut a new ARK release after a meaningful chunk of work lands on main. Use when the user says to cut a release, tag a version, or ship what's on main.
---

# Cutting an ARK release

1. Confirm `main` is clean and pushed (`git status`, `git log origin/main..HEAD`).
2. Check CI on the latest commit is green before tagging:
   `curl -sS "https://api.github.com/repos/raven-clown/ark/commits/<sha>/check-runs" -H "Accept: application/vnd.github+json"`
   Every check must be `success` or `skipped`. If anything failed, fix it first, push, and recheck.
3. Pick the next version. Releases are `vMAJOR.MINOR.PATCH`, bump MINOR for a
   new phase/feature, PATCH for a fix-only release. Check the existing series
   with `git tag -l` and `git tag -n99` for what each one covered.
4. Tag and push:
   ```
   git tag -a vX.Y.Z -m "vX.Y.Z: <one line of what shipped>" <sha>
   git push origin vX.Y.Z
   ```
5. If the tag push is rejected with a 403 (some session types cannot push
   tags or create GitHub releases), tell the user directly instead of
   retrying: the tag exists locally, ask them to push it or grant more access.

Never tag a commit whose CI is red or unknown. Never invent a changelog beyond
what's actually in the commits since the last tag.
