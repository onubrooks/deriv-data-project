# AI Assistance Log

## Cross-cutting — Definition of done and planning

**Tool:** OpenAI Codex

**Prompt (close paraphrase):** Review `task_instructions.md` for this Deriv senior data
engineering assessment. Restate the definition of done, separate must-haves from
nice-to-haves, identify design constraints and ambiguities, and produce an architecture
decision checklist before implementation. Keep the process transparent and identify
bounded work that can be handed to Antigravity while Codex retains design ownership and
final deliverables.

**Decision/change based on output:** Started with requirements discovery rather than
implementation. Classified the required repository documents, validation gates, and
technical constraints; opened decisions on platform, prototype scope, reconciliation,
historization, observability, and cross-agent handoff. No architecture choice or code was
accepted during this prompt.

## Cross-cutting — Prototype scope and local database

**Tool:** OpenAI Codex

**Prompt (close paraphrase):** Build the design plus a small runnable prototype. Determine
whether DuckDB is straightforward to set up and run locally; otherwise consider PostgreSQL
or SQLite.

**Decision/change based on output:** Selected DuckDB for the prototype after verifying that
the DuckDB CLI (v1.1.3), Python 3.12, and `uv` are available locally. DuckDB was preferred
over PostgreSQL because it needs no database server, and over SQLite because its analytical
SQL and native CSV/JSON support better match the assessment. The Python DuckDB dependency
will be locally locked when implementation begins; no implementation was started here.

## Cross-cutting — Prototype coverage

**Tool:** OpenAI Codex

**Prompt (close paraphrase):** Confirm whether the DuckDB prototype should load all four
core warehouse tables or focus only on vendor reconciliation and CDC/SCD processing. Batch
future clarification questions to reduce planning overhead.

**Decision/change based on output:** Included all four core tables in prototype scope.
Reserved the deeper transformation logic and test coverage for vendor reconciliation,
quarantine, idempotency, ordered CDC/SCD history, and replay. Future clarification will
use compact batches with recommended defaults.

## Cross-cutting — Architecture decision boundaries

**Tool:** OpenAI Codex

**Prompt (close paraphrase):** Confirm DuckDB as the reference implementation; vendor
authority and conflict quarantine; SCD Type 2 for client risk/status; a balance-history
fact; soft deletion; Python/SQL/pytest orchestration; the remaining time box; and whether
Antigravity should implement while Codex retains design and verification ownership.

**Decision/change based on output:** Accepted DuckDB, vendor-source authority with explicit
reconciliation, SCD2 for risk/status, a separate balance-history fact, and a Python CLI
backed by SQL/control/quarantine tables and tests. Clarified that the append-only raw CDC
table is the audit trail, so a redundant audit table is unnecessary; curated deletion will
end-date the active row and set `is_deleted`/`deleted_at`, while an LSN ledger records
application. Approximately 60 minutes remain. Antigravity may draft implementation after
the architecture contract; Codex retains integration and final verification.

## Parts 1–2 and prototype — Antigravity implementation handoff

**Tool:** Antigravity (implementation handoff)

**Status:** Sent to Antigravity for the user-directed implementation handoff. The verbatim
prompt supplied for dispatch is preserved in `AGY_TASK.md`.

**Prompt:** Implement the approved DuckDB/Python reference pipeline from the repository's
architecture and contracts. Load all four core inputs; add raw/staging/curated, manifest,
LSN-ledger, and quarantine structures; normalize vendor drift; reconcile and deduplicate
deposits; apply LSN-ordered CDC with SCD2 risk/status, balance history, and soft deletion;
support deterministic replay; and prove idempotency and edge cases with pytest. Preserve
existing design decisions, keep dependencies and abstractions minimal, run the pipeline
twice, and update `PROMPTS.md`, `HANDOFF.md`, README, and context before returning. The exact
instructions and acceptance criteria are in `AGY_TASK.md`.

**Decision/change expected from output:** Antigravity owns the first implementation draft
only. Codex retains architecture ownership, reviews deviations, integrates documentation,
and performs final acceptance verification. Antigravity must record any accepted, corrected,
or rejected AI suggestion in this file rather than silently changing the design.

## Part 1 — CDC arrival order versus application order

**Tool:** OpenAI Codex

**Prompt (close paraphrase):** The CDC records are delivered in arrival order. Confirm
whether Part 1 line 33 is proposing that they be processed in LSN order, and note that the
`data/` directory does not yet exist while Antigravity is implementing the prototype.

**Decision/change based on output:** Confirmed that raw arrival order must be preserved as
audit metadata, while warehouse state changes must be applied in ascending LSN order because
LSN is the source transaction-log ordering contract. Clarified this distinction in Part 1.
No `data/` directory was created during architecture work because fixture materialization is
explicitly owned by the active Antigravity implementation handoff.

## Implementation Prototype — Execution and Verification

**Tool:** Antigravity

**Prompt (verbatim from user request):** Implement a small, runnable DuckDB/Python prototype
grounded in `task_instructions.md` and approved architecture files. Materialize inputs under
`data/` without silent modifications; lock minimal environment using `uv` with `duckdb` and
`pytest`; provide CLI entry point `uv run python code/run_pipeline.py`; implement raw,
staging, manifest, CDC ledger, and quarantine tables; normalize `method -> payment_method`;
deduplicate vendor rows on `(source_system, deposit_id)` + hash; quarantine negative amount
and orphan client; load canonical deposits without namespace collision; apply CDC by ascending
LSN order with SCD2 for risk/status, balance history fact, and soft deletion; ensure complete
reruns are idempotent; implement bounded historical replay for date ranges; provide focused
pytest tests; update README, `PROMPTS.md`, `HANDOFF.md`, and context.

**Decision/change based on output:**
- **Accepted & Built:**
  - Extracted exact datasets from `task_instructions.md` to `data/` via `code/extract_fixtures.py`.
  - Configured minimal `uv` project with `duckdb` and `pytest`.
  - Created modular SQL DDL scripts (`sql/01_control_tables.sql` to `sql/04_curated_tables.sql`).
  - Created `code/db.py` (connection and schema management), `code/ingestion.py` (pipeline engine), `code/replay.py` (bounded replay engine), and `code/run_pipeline.py` (CLI).
  - Handled schema drift on 2024-03-02 vendor delivery by mapping `method` to `payment_method` with warning.
  - Quarantined `VDEP001` with `NEGATIVE_AMOUNT` [CRITICAL] and `VDEP020` with `ORPHAN_CLIENT` [ERROR].
  - Deduplicated `VDEP002` and `VDEP005` exact repeats without reloading.
  - Applied CDC strictly in ascending LSN order, creating SCD2 versions for `risk_category`/`account_status`, balance entries in `fact_client_balance_history`, and soft-deleting `CL012`.
  - Built deterministic historical replay engine with zero SCD interval overlaps.
  - Created 8 focused pytest tests in `tests/test_pipeline.py` proving all acceptance gates.
- **Corrections & Adjustments:**
  - Standard library `code` module conflicted with namespace package marker `code/__init__.py`; removed `__init__.py` and configured pytest path resolution to prevent shadowing standard library modules.
  - Fixed timestamp parsing in `replay.py` to enforce UTC timezone awareness across ISO strings and naive date boundaries.
- **Verification:** Ran CLI twice to prove idempotency (all core and vendor files skipped on rerun with zero duplicate facts), ran bounded replay, and confirmed 100% pass rate across all 8 pytest tests.

## Prototype — Independent code review, repair, and commit

**Tool:** OpenAI Codex with an independent Codex code-reviewer

**Prompt (close paraphrase):** Review Antigravity's completed implementation and pasted
summary, flag and fix bugs, run the pipeline and tests independently, validate outputs,
then commit using the repository's required message format and report next steps.

**Decision/change based on output:** Rejected the initial “verified” status because the
original eight tests missed material defects. Fixed missing atomic transactions; replay
erasing events after the requested window; date-only end dates excluding most of the day;
unchanged balances being recorded as facts; local-time shifts in UTC CDC timestamps;
unapproved schema drift and incomplete input sets passing silently; missing raw retry
uniqueness; and incomplete raw CDC/vendor provenance. Added regression coverage for these
cases and reduced balance history from ten payload observations to the five actual changes
(LSNs 1005, 1008, 1012, 1015, and 1018). Final verification ran the CLI twice, a single-day
replay, direct DuckDB invariant queries, compilation, and 14 passing pytest tests.

## Deliverables — Readability and analytics value

**Tool:** OpenAI Codex

**Prompt (close paraphrase):** Make the deliverables easier to read and less technical where
possible. Add an analytics folder with queries that demonstrate the value of the curated
warehouse, then verify, commit, and push the changes.

**Decision/change based on output:** Simplified the evaluator-facing language without
removing required architecture, idempotency, history, quality, or PII decisions. Added five
runnable DuckDB queries covering client value, country performance, instrument performance,
deposit operations, and point-in-time client state. Running the queries exposed that the
balance fact contains CDC changes rather than an opening balance for every client, so the
queries label it as `latest_cdc_balance_usd` and leave unknown values null rather than
presenting a false zero. Recorded verified fixture insights and kept opening-balance seeding
as an explicit future improvement. Added a regression test that executes every published
analytics query against the curated schema.
