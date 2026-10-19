## Summary

<!-- Required: What does this PR do? One or two clear sentences. -->


## Motivation

<!-- Required: Why is this change needed? Link to the issue it closes. -->

Closes #

## Implementation notes

<!-- Required: How did you implement this? What was non-obvious? -->


## Questions for the reviewer

> **These questions must be answered before your PR will be reviewed.**
> Replace each `…` with your actual answer. Unanswered questions block merge.

**Q1 — What existing tests cover the code you changed?**
A: …

**Q2 — What new tests did you add, and what cases do they cover?**
A: …

**Q3 — Does this change affect any authenticated or unauthenticated API paths? If yes, how?**
A: …

**Q4 — Did you run `golangci-lint run` locally and get 0 issues?**
A: …

**Q5 — Is this the only open PR you have against `development`?**
A: …

## Checklist

- [ ] `go test -race -count=1 -timeout=120s ./...` passes locally
- [ ] `golangci-lint run --timeout=5m` reports 0 issues
- [ ] `gofmt -s -l .` reports no unformatted files
- [ ] All five questions above are answered (not left as `…`)
- [ ] No secrets or credentials in code / config
- [ ] New dependencies reviewed and justified
- [ ] Related issue linked in "Closes #" above
