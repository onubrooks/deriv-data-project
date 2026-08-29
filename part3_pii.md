# Part 3 — PII Handling

Preserve `email`, `date_of_birth`, and `full_name` only in the encrypted raw/restricted
staging zone, then tokenize identifiers and expose masked values when publishing curated
analytics views. Grant unmasked column access only to audited Compliance roles through
least-privilege RBAC, while pipeline service accounts receive write-only scoped access and
Analytics/BI users receive masked views; encrypt all PII in transit and at rest.
