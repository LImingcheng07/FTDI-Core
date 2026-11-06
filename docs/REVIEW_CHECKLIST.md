# Pull request / handoff review template

## Scope
- Task ID and intent:
- Changed files and brief interface changes:
- Schema migration / data contract change? (default: NO)
- Changes to the frozen compatibility adapter? (default: NO)

## Evidence
- `git status --short`:
- `git diff --stat`:
- `python -W error::ResourceWarning -m unittest discover -s tests -q`:
- `reference_s07_0041` suite:
- Relevant failure injection and corruption tests:
- Source/hash/provenance assertions:

## Safety checks
- Any hidden drop/gap skip or invented recovery?
- Any premature ACK or false COMPLETE?
- Any source, archive, or consumer ledger deletion?
- Any new network or credential request?
- Recovery and rollback plan:
- Windows-only behavior not verified locally:

## Release judgment
- Local research gate: PASS / FAIL / NOT YET TESTED
- Production gate: NOT CERTIFIED by default
