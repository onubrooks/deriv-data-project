# AGENTS.md

Guardrails for any AI coding agent (Claude Code, Cursor, Codex, AntiGravity, Copilot, or similar)
working on data pipeline, warehouse, or data platform code in this repository.
Apply these by default — do not wait to be asked.

## Cross-agent session handoff

Before ending a Codex or Antigravity session, update `PROMPTS.md` with the prompt and the
decision taken from it, update `HANDOFF.md` with current state/decisions/next action, and
refresh the relevant `.omx/context/` snapshot. Update the README closeout only when a unit
of work has actually been completed; do not claim planned work as built.

## Non-negotiable principles

### 1. Pipelines don't fail silently

- Every pipeline must be idempotent: re-running it after a partial failure
  must never create duplicate rows or corrupt state.
- Prefer explicit upsert/merge patterns over blind inserts.
- Every failure mode produces a visible signal (log, alert, dead-letter) —
  never a quiet skip.

### 2. Data contracts are explicit, not implied

- Every producer-consumer boundary gets a written contract: expected schema,
  SLA (when data must arrive), SLO (freshness/completeness thresholds).
- Breaking schema changes require a version bump and an explicit migration
  note — never a silent column rename or type change.
- Propose the contract before writing the transform, not after.

### 3. Observability is built in, not bolted on

- Every pipeline includes, from the first commit: freshness checks,
  completeness/row-count checks, schema drift detection, and basic lineage
  (what feeds this, what this feeds).
- Alerting thresholds are stated explicitly in code or config — not left as
  a TODO.

### 4. Dimensional modeling is a deliberate choice

- Before modeling data, state which pattern is being used (Kimball star
  schema, Data Vault, Medallion) and *why*, given the actual query patterns
  and team maturity — never default to one out of habit.
- Justify the grain of every fact table explicitly, in a comment or doc.

### 5. PII and access control are addressed, not assumed

- Any table or field that could contain PII, financial identifiers, or
  regulated data must be flagged in code/comments.
- State the access control and masking approach even if it isn't fully
  implemented in a given task — silence on this is not acceptable.

### 6. Warehouse cost is a design input, not an afterthought

- Partition and cluster by actual query pattern, not by ingestion
  convenience.
- Flag any full-table scan, unbounded join, or unpartitioned large table the
  agent introduces.

### 7. AI-assisted development is a daily habit, reviewed — not rubber-stamped

- Generate code fast, but every AI-generated block gets a one-line
  human-readable justification of why it's correct, not silent acceptance.
- Flag explicitly any place where the agent is uncertain or made an
  assumption, rather than presenting a guess as fact.

## Pre-completion checklist

Run this before calling anything "done":

- [ ] Can this pipeline be re-run without creating duplicates?
- [ ] Is there a data quality check at the ingestion boundary (not only at
      the end)?
- [ ] Is at least one tradeoff made under constraints named explicitly
      (e.g. "skipped partitioning here given time — would partition on
      transaction_date at scale")?
- [ ] If this touches PII or financial data, is there at least a stated
      sentence on access control, even if not implemented?

## README requirement

Every unit of work ends with an updated README containing exactly three
things:

1. What was built.
2. What was deliberately cut, and why (time, scope, or risk tradeoff).
3. What would be done differently with more time.

Do this before considering the task complete. Never leave it for "later" —
it is the one artifact that outlives the session.
