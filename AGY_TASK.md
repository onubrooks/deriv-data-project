# Antigravity Implementation Handoff

## Status

This is the exact implementation prompt prepared for Antigravity. The user will paste it
into Antigravity; after dispatch, treat it as sent and preserve it verbatim in `PROMPTS.md`.

## Prompt

You are implementing the bounded prototype for a monitored Deriv senior data engineering
assessment. Work in `/Users/onuh/Documents/Work/Open Source/deriv-data-project`.

Read these files completely before editing:

1. `AGENTS.md`
2. `task_instructions.md`
3. `HANDOFF.md`
4. `.omx/specs/deep-interview-deriv-assessment.md`
5. `part1_pipeline.md`
6. `part2_data_model.md`
7. `docs/data_contracts.md`
8. `PROMPTS.md`

Implement a small, runnable DuckDB/Python prototype. You own the first implementation draft;
do not redesign the approved architecture. You are not alone in the repository: preserve
existing edits and do not revert or rewrite the design documents unless an implementation
contradiction makes a minimal correction necessary. Report any such correction explicitly.

Required scope:

- Materialize the embedded inputs from `task_instructions.md` into reproducible fixture files
  under `data/`, or write a deterministic extractor that does so. Do not hand-copy values in
  a way that silently changes the supplied data.
- Use `uv` to create/lock the minimal Python environment. Add only `duckdb` and `pytest` unless
  the standard library suffices; do not add a framework.
- Provide one CLI entry point, preferably `uv run python code/run_pipeline.py`, that creates a
  local DuckDB database and loads all four core inputs.
- Implement immutable/raw source tables, typed staging, `ingestion_file_manifest`,
  `cdc_processing_ledger`, and a quarantine table.
- Normalize the approved vendor schema alias `method → payment_method`.
- Deduplicate vendor rows by `(source_system, deposit_id)` plus canonical row hash. Exact
  repeats must not reload; conflicting repeats must quarantine rather than overwrite.
- Quarantine the negative vendor amount and orphan client with explicit reason codes.
- Load canonical deposits without collapsing `DEP…` and `VDEP…` namespaces.
- Apply CDC by ascending LSN, never arrival order. Implement SCD2 for `risk_category` and
  `account_status`, append balance changes to `fact_client_balance_history`, and soft-delete
  by end-dating/flagging the current client version.
- Make complete reruns idempotent. Manifest/ledger success state and curated mutations must
  be transactional where DuckDB permits.
- Provide a bounded historical replay command or callable function for a date range. It must
  rebuild affected history deterministically without overlapping SCD intervals.
- Add focused pytest coverage proving: second run creates no duplicates; vendor schema drift
  is normalized; repeated vendor rows are deduplicated; invalid/orphan rows quarantine;
  CDC follows LSN order; delete history is retained; and replay is deterministic.
- Add concise run instructions and actual verification results to the README while retaining
  exactly its three required sections: what was built, what was deliberately cut and why,
  and what would be done differently with more time.

Keep the implementation intentionally small. Prefer SQL files and simple Python orchestration;
do not create repositories/services/classes unless genuinely needed. Every generated block
must have a brief human-readable justification in nearby documentation or comments.

Before finishing, run the CLI twice, run pytest, inspect key row counts/invariants, and report
the commands and outputs. Do not claim success if a check fails.

AI provenance is mandatory: append an entry to `PROMPTS.md` for every substantive prompt you
receive or issue. Include the part affected, actual prompt or close paraphrase, what you
accepted/changed/rejected, and explicitly identify Antigravity as the tool. Update `HANDOFF.md`
and the relevant `.omx/context/` snapshot before ending the session so Codex can resume.

Return a concise handoff listing changed files, tests/results, simplifications, assumptions,
and remaining risks. Do not commit unless the user explicitly asks.
