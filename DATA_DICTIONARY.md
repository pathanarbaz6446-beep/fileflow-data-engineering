# Data Dictionary

## Source entity: orders

**Grain:** one row per immutable order event.  
**Primary key:** `event_id` (producer-provided, stable, globally unique).  
**Relationship:** `customer_id` references the synthetic customer domain `C00001` through `C10000`.  
**Nullability:** all source business fields are required.  
**History:** append-only events; do not silently overwrite previous events.

| Field | Source type | Curated type | Nullable? | Rule |
|---|---|---|---|---|
| event_id | string | string | No | non-empty; unique in curated zone |
| customer_id | string | string | No | must match `C` followed by 5 digits, within C00001–C10000 |
| event_timestamp | string | UTC ISO-8601 | No | must parse as timestamp |
| amount | string/number | float | No | numeric and >= 0 |
| currency | string | string | No | three uppercase alphabetic characters |

Pipeline metadata fields (`source_file`, `run_id`, `ingested_at`, `event_date`) are added downstream. `event_date` is derived from `event_timestamp` after UTC normalization and is the partition key.

## Operational record

Each run record contains `run_id`, `started_at`, `ended_at`, `status`, `files_seen`, `files_processed`, `files_skipped`, `input_rows`, `valid_rows`, `rejected_rows`, `duplicate_rows`, `curated_added_rows`, `elapsed_seconds`, and `failure_reason`.

## Reject reason examples

`missing_event_id`, `missing_customer_id`, `missing_event_timestamp`, `invalid_event_timestamp`, `missing_amount`, `invalid_amount`, `negative_amount`, `missing_currency`, `invalid_currency`, `invalid_customer_reference`, `duplicate_event_id`, `schema_missing_columns`, `schema_unexpected_columns`, `malformed_csv`.
