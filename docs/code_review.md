# Independent Prototype Review

## Initial verdict

Changes were requested despite the original eight passing tests. The review found that
successful-path counts were correct but several failure and replay paths contradicted the
documented contracts.

## Findings and resolution

| Severity | Finding | Resolution |
|---|---|---|
| High | Pipeline wrote curated data and SUCCESS control state without an explicit transaction. | Wrapped publication in commit/rollback and added a rollback regression test. |
| High | Bounded replay deleted later client versions but reapplied only in-window events. | Rebuild affected clients from the boundary through their latest event; test a later event. |
| High | Date-only replay treated the end date as midnight. | Expand date-only end dates through `23:59:59.999999`; test a one-day window. |
| High | Balance fact stored unchanged balances carried in CDC payloads. | Compare `before` and `after`; retain only five actual balance changes. |
| High | UTC CDC timestamps shifted under the local DuckDB session timezone. | Set every connection session to UTC and assert exact effective timestamps. |
| High | Raw retries could duplicate rows if manifest state was absent. | Add file-row unique indexes and conflict-safe raw inserts. |
| Medium | Unknown vendor columns and partial input directories were not rejected. | Validate exact normalized schema and all eight required input files. |
| Medium | Raw vendor/CDC provenance was incomplete. | Preserve raw vendor payload and CDC arrival sequence; reject conflicting LSN payloads. |

## Final evidence

- Default CLI: first run loads expected records; second run skips every processed input.
- Single-day replay for `2024-11-20`: succeeds and rebuilds affected downstream history.
- Direct invariants: zero duplicate current clients, overlapping SCD intervals, duplicate
  deposit keys, or duplicate balance LSNs.
- Tests: 14 passed.
