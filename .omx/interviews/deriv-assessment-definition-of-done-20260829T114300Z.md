# Interview Summary — Deriv Assessment

Three clarification rounds established the definition of done, prototype breadth, and
architecture boundaries. The submission will contain all required documents plus a small
DuckDB prototype loading all core tables. Difficult-path depth is reserved for vendor
reconciliation, data quarantine, idempotency, ordered CDC/SCD history, deletion, and replay.

The initial broad prototype scope was pressure-tested against the remaining time. The
resolution is shallow ingestion for all four core tables, focused implementation/tests for
the assessed failure modes, and no simulated cloud infrastructure.

Final ambiguity: 14% (standard threshold: 20%). Non-goals and autonomous decision
boundaries are explicit.
