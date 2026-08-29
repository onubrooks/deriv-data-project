# Approach & Communication Playbook

## Thinking out loud with AI, for any timed technical exercise or real engineering task

## Phase 0 — Get the Definition of Done first

Before touching a keyboard, ask — of the brief, the AI, or the stakeholder:

- "What does 'done' look like here, concretely?"
- "What's the one thing this absolutely must do, versus what's nice-to-have?"
- "Is there a deadline-shaped constraint I should design around — time box,
  data volume, audience?"

Prompt template:
> "Given this brief, restate what a complete, acceptable solution looks
> like, and flag anything ambiguous before I start."

## Phase 1 — Orient (TOGAF-lite)

Four questions, in this order, spoken out loud:

1. **Target state** — where do we want to be? (ideal end state, ignoring
   constraints for a moment)
2. **Current state** — where are we now? (what exists, what's given, what's
   broken)
3. **Gap** — what's actually stopping us getting from current to target?
   (technical, time, information)
4. **Requirements** — given the gap, what do we need to gather or decide
   before designing anything?

## Phase 2 — Architect out loud

- Propose 2-3 real approaches, not one.
- For each, one sentence on what it optimizes for and what it costs.
- Pick one and say why, explicitly, as a tradeoff sentence — e.g. "I'm
  choosing X over Y because time matters more than completeness here."
- This is the single highest-signal moment in any AI-assisted assessment.
  Don't rush past it to get to typing.

## Phase 3 — Define the contract before the code

- State the expected input/output shape of the pipeline or function before
  writing it, even informally.
- If the output feeds another system or a reviewer, say explicitly what
  they can rely on and what they can't yet.

## Phase 4 — Build, hitting the non-negotiables

Regardless of task size, hit these before calling anything done:

- [ ] Idempotency — can this run twice safely?
- [ ] A data quality check at the ingestion boundary — repeat at each major
      transform stage if time allows.
- [ ] One tradeoff explicitly named out loud.
- [ ] One sentence on PII/access control if the data resembles anything
      regulated or personal.

## Phase 5 — Document the artifact that outlives you

Before ending any session, update a README with exactly three things:

1. What was built.
2. What was deliberately cut, and why.
3. What you'd do differently with more time.

This is the thing anyone reviewing afterward actually reads. Never skip it
for the sake of five more minutes of coding.

## Communication habits to hold throughout

- Narrate the plan before the first line of code.
- Narrate every moment you accept or reject an AI suggestion, and why.
- When stuck, say what you're stuck on out loud before searching or
  prompting again — it's a stronger signal than silence.
