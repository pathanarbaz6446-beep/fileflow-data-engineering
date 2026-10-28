# FileFlow: Reliable File Ingestion & Data Quality Pipeline

A local, reproducible data-engineering project demonstrating raw, validated, curated and quarantine zones; schema/business validation; duplicate handling; date partitioning; idempotent reruns; operational metrics; tests; and reconciliation.

## Architecture

```text
data/incoming/*.csv
       |
       v
  RAW zone (immutable per-file copy)
       |
       v
 schema + type + business/reference checks
       |                         |
       v                         v
VALIDATED records           QUARANTINE records
       |
       v
deterministic event-key deduplication
       |
       v
CURATED zone partitioned by event_date (year/month/day)
       |
       v
run_metrics.jsonl + manifest.json + reconciliation report
```

## Requirements

- Python 3.10+
- pandas (only runtime dependency)

## Quick start (Windows PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m fileflow generate --rows 1000 --seed 42
python -m fileflow run
python -m unittest discover -s tests -v
```

On macOS/Linux, activate with `source .venv/bin/activate`.

Run with a larger synthetic batch:

```bash
python -m fileflow generate --rows 100000 --seed 42
python -m fileflow run
```

The generator writes CSV files to `data/incoming/`. It deliberately includes some duplicate event IDs, missing required values, invalid amounts, unknown customer references, and older event dates. Invalid cases are labelled in `data/generator_truth.csv` for testing only; the pipeline does not use that label.

## Output zones

- `data/raw/`: exact copies of accepted-to-processing source files.
- `data/validated/`: records passing schema and business validation before cross-file deduplication.
- `data/curated/event_year=YYYY/event_month=MM/event_day=DD/`: final unique events, partitioned by event date.
- `data/quarantine/`: rejected rows plus `reject_reason`.
- `data/ops/manifest.json`: per-file processing ledger keyed by SHA-256 content hash.
- `data/ops/run_metrics.jsonl`: one operational record per run.
- `data/ops/reconciliation.json`: latest run counts and checks.

## Data contract / data dictionary

Entity: `orders` (one row = one order event; events are append-only).

| Column | Type | Rules |
|---|---|---|
| event_id | string | Required stable event key; unique in curated output |
| customer_id | string | Required; must exist in the small reference set `C001`–`C10000` |
| event_timestamp | ISO-8601 timestamp | Required and parseable; UTC-normalized |
| amount | decimal number | Required; must be >= 0 |
| currency | string | Required; exactly three uppercase letters |
| source_file | string | Source basename for lineage |
| run_id | string | Identifier of processing run |
| ingested_at | ISO-8601 UTC timestamp | Pipeline ingestion timestamp |
| event_date | YYYY-MM-DD | Derived from event_timestamp in UTC; drives partitions |

Deterministic key: producer-supplied `event_id`. The pipeline does not invent a new event ID because doing so could conceal repeated delivery. The event grain is one order event, not one customer. The customer reference is intentionally synthetic and represented as a range for this demo.

## Behavior and assumptions

- **Idempotency:** the manifest stores the SHA-256 of each successfully processed input file. Re-running unchanged input skips it. Curated rows are also deduplicated by `event_id` against existing curated partitions, protecting against a repeated event in a different file.
- **Duplicate policy:** keep the first event for a given `event_id`; subsequent copies are quarantined with `duplicate_event_id`. In this demo, deterministic file-name ordering defines “first” when multiple files are processed in one run.
- **Late events:** events are partitioned by their event date, not arrival date. A late file can therefore populate an older date partition. Re-run is safe.
- **Bad records:** row-level schema/business errors go to quarantine with original fields and a reason. A missing required column or unexpected extra column is a file-level schema failure; the file is not marked processed, so it can be corrected and retried.
- **Empty file:** header-only CSV is a valid empty batch and is recorded as processed.
- **Partial failure/retry:** successful files are committed individually in the manifest. A failed file is not marked complete; fix it and rerun. This is a local demo, not a transactional distributed system.
- **History:** events are append-only; existing event records are not overwritten when a new event arrives. Corrections should arrive as new events with a new `event_id` and an explicit relationship in a production contract.
- **Storage:** local folders emulate object-storage prefixes. An S3/Azure adapter could replace the file listing/copy layer without changing validation rules.
- **Schema change:** strict column checks intentionally fail fast rather than silently dropping unexpected data.
- **Known limitation:** manifest updates and output writes are not a multi-file atomic transaction. Production deployments should use a transactional table format or database ledger, object versioning, and locking for concurrent workers.

## Quality checks and reconciliation

The pipeline writes input, valid, rejected, duplicate, curated-added and elapsed-time counts. It asserts:

`input_rows = valid_rows + rejected_rows`

For valid rows, `valid_rows = curated_added_rows + duplicate_rows` for a run with no existing-key overlap beyond the file-level processing order. Existing curated-key collisions are also counted as duplicates. Use `data/ops/reconciliation.json` for the exact run-level reconciliation.

## Tests

```bash
python -m unittest discover -s tests -v
```

Tests cover happy path, empty batch, malformed CSV/schema, missing required values, duplicate events, negative amounts, invalid customer references, late-arriving data, and safe reruns.

## Performance note

The generator supports 100,000+ records. The current implementation uses pandas for straightforward, vectorized validation and reads one CSV at a time to limit memory use. A sensible next optimization is chunked CSV processing and batched manifest writes for very large files. Measure on your machine; do not claim benchmark numbers without running them locally.

## Suggested GitHub submission

1. Create a **public** repository named `fileflow-data-engineering`.
2. Upload the contents of this folder (not just the ZIP).
3. Keep `data/incoming/` sample CSVs and generated outputs if useful, but do not commit large 100k-row generated files.
4. Commit source, tests, `README.md`, `DATA_DICTIONARY.md`, `DESIGN_NOTES.md`, `requirements.txt`, and `.gitignore`.
5. Run the tests and quick-start commands from a clean environment before submitting.
