# Execution-ready Specification — Deriv Assessment

## Outcome

Produce a clear, defensible repository containing the three required assessment documents,
AI provenance, navigation README, and a reproducible DuckDB prototype grounded in the
embedded source records.

## In scope

- Platform-neutral production architecture and local DuckDB reference implementation.
- Ingestion of signup, profile, deposit, and trade core tables.
- Vendor deposit normalization, idempotency, reconciliation, and quarantine.
- LSN-ordered CDC, SCD2 risk/status, balance history, soft deletion, and replay safety.
- Explicit contracts, data quality severities/actions, observability, PII, cost, and tests.

## Non-goals

- Cloud deployment, a real scheduler, UI, generalized framework, or exhaustive coverage.
- Silent conflict resolution, hard deletion, or invented source guarantees.

## Key decisions

- Vendor is authoritative only for vendor-origin deposits. Conflicts are recorded, not
  silently overwritten.
- `risk_category` and `account_status` use SCD2. `account_balance_usd` uses an append-only
  balance-history fact.
- Deletes end-date the current dimension row and populate `is_deleted`/`deleted_at`.
- Raw CDC is the audit trail; an application ledger makes LSN processing idempotent.
- A Python CLI, SQL files, manifests, quarantine tables, and pytest form the prototype.

## Acceptance criteria

- All evaluator-required files exist and are self-contained.
- Reruns produce no duplicate canonical deposits, SCD versions, or balance events.
- CDC is applied by LSN rather than arrival order.
- Schema drift, duplicate files/rows, late records, negative amounts, and orphan clients
  produce the documented outcomes.
- Historical replay preserves non-overlapping history and is reproducible.
- Tests and a one-command execution path pass locally.

## Handoff

Antigravity may draft prototype implementation after reading `task_instructions.md`,
`HANDOFF.md`, this spec, and `PROMPTS.md`. Codex retains design integration and final QA.
