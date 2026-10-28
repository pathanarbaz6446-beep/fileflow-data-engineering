# Design Notes and Trade-offs

- **CSV + pandas:** easy to run locally and inspect. At much larger scale, use chunking/Spark and a durable table format.
- **File hash ledger:** simple idempotency for unchanged files. Event-key deduplication is a second guard across different files.
- **Strict schema:** unexpected columns fail fast to avoid silently accepting producer contract changes.
- **Quarantine over drop:** each rejected row keeps source fields and lineage plus a reason.
- **Event-date partitioning:** late events land in the correct historical partition rather than today's arrival partition.
- **Per-file completion:** completed files are skipped on retry; failed files remain eligible. Outputs are append-oriented. Concurrent runs are not supported in this demo.
- **Deterministic duplicate choice:** sorted filename order and first occurrence. A production system may need event version/updated_at rules.
- **Synthetic reference keys:** the project uses a bounded customer-ID domain rather than an external dimension table, keeping setup reproducible.
