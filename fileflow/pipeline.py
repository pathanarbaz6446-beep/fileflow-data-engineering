from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import shutil
import time
import uuid
import pandas as pd

EXPECTED = ["event_id", "customer_id", "event_timestamp", "amount", "currency"]
CUSTOMER_MIN, CUSTOMER_MAX = 1, 10000

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def sha256_file(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _load_json(path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default

def _write_json_atomic(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)

def _append_csv(frame, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        return
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)

def _existing_curated_ids(root):
    ids = set()
    folder = root / "curated"
    if folder.exists():
        for path in folder.rglob("*.csv"):
            try:
                if "event_id" in pd.read_csv(path, usecols=lambda c: c == "event_id").columns:
                    ids.update(pd.read_csv(path, usecols=["event_id"])["event_id"].dropna().astype(str))
            except (ValueError, pd.errors.EmptyDataError):
                continue
    return ids

def validate_frame(frame):
    """Return (valid, rejected); retain source values and explain each rejection."""
    if list(frame.columns) != EXPECTED:
        missing = sorted(set(EXPECTED) - set(frame.columns))
        extra = sorted(set(frame.columns) - set(EXPECTED))
        raise ValueError(f"schema mismatch; missing={missing}; unexpected={extra}")
    df = frame.copy()
    reasons = [[] for _ in range(len(df))]
    def add(mask, reason):
        for i, yes in enumerate(mask.fillna(False).tolist()):
            if yes:
                reasons[i].append(reason)
    for col, reason in [
        ("event_id", "missing_event_id"),
        ("customer_id", "missing_customer_id"),
        ("event_timestamp", "missing_event_timestamp"),
        ("amount", "missing_amount"),
        ("currency", "missing_currency"),
    ]:
        add(df[col].astype("string").str.strip().eq("").fillna(True), reason)
        add(df[col].isna(), reason)
    parsed = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
    add(parsed.isna() & df["event_timestamp"].notna() & df["event_timestamp"].astype(str).str.strip().ne(""), "invalid_event_timestamp")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    add(amount.isna() & df["amount"].notna() & df["amount"].astype(str).str.strip().ne(""), "invalid_amount")
    add(amount.lt(0), "negative_amount")
    currency = df["currency"].astype("string")
    add(~currency.str.fullmatch(r"[A-Z]{3}", na=False), "invalid_currency")
    customer_num = df["customer_id"].astype("string").str.extract(r"^C(\d{5})$", expand=False)
    add(customer_num.isna() | pd.to_numeric(customer_num, errors="coerce").lt(CUSTOMER_MIN) | pd.to_numeric(customer_num, errors="coerce").gt(CUSTOMER_MAX), "invalid_customer_reference")
    reason_text = [";".join(dict.fromkeys(r)) for r in reasons]
    valid_mask = pd.Series([r == "" for r in reason_text], index=df.index)
    valid = df.loc[valid_mask].copy()
    rejected = df.loc[~valid_mask].copy()
    rejected["reject_reason"] = [reason_text[i] for i, ok in enumerate(valid_mask.tolist()) if not ok]
    if not valid.empty:
        valid["event_timestamp"] = parsed.loc[valid_mask].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        valid["amount"] = amount.loc[valid_mask].astype(float)
        valid["event_date"] = parsed.loc[valid_mask].dt.strftime("%Y-%m-%d")
    return valid, rejected

def run_pipeline(input_dir="data/incoming", root="data"):
    input_dir, root = Path(input_dir), Path(root)
    ops = root / "ops"
    manifest_path = ops / "manifest.json"
    manifest = _load_json(manifest_path, {"processed_files": {}})
    run_id = str(uuid.uuid4())
    started = utc_now()
    tick = time.perf_counter()
    metrics = {
        "run_id": run_id, "started_at": started, "ended_at": None, "status": "success",
        "files_seen": 0, "files_processed": 0, "files_skipped": 0,
        "input_rows": 0, "valid_rows": 0, "rejected_rows": 0,
        "duplicate_rows": 0, "curated_added_rows": 0, "elapsed_seconds": 0.0,
        "failure_reason": None
    }
    incoming = sorted(input_dir.glob("*.csv")) if input_dir.exists() else []
    metrics["files_seen"] = len(incoming)
    existing_ids = _existing_curated_ids(root)
    try:
        for path in incoming:
            digest = sha256_file(path)
            if manifest["processed_files"].get(path.name) == digest:
                metrics["files_skipped"] += 1
                continue
            try:
                frame = pd.read_csv(path, dtype=str, keep_default_na=False, on_bad_lines="error")
            except Exception as exc:
                raise ValueError(f"malformed_csv in {path.name}: {exc}") from exc
            # Strict schema failure is file-level: don't mark it complete.
            valid, rejected = validate_frame(frame)
            metrics["input_rows"] += len(frame)
            metrics["valid_rows"] += len(valid)
            metrics["rejected_rows"] += len(rejected)
            raw_path = root / "raw" / path.name
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            if not raw_path.exists():
                shutil.copy2(path, raw_path)
            # Add lineage before writing validated/quarantine rows.
            for data in (valid, rejected):
                if not data.empty:
                    data["source_file"] = path.name
                    data["run_id"] = run_id
                    data["ingested_at"] = utc_now()
            _append_csv(valid, root / "validated" / f"{path.stem}.csv")
            _append_csv(rejected, root / "quarantine" / f"{path.stem}_rejected.csv")
            duplicate_indices = []
            for idx, event_id in valid["event_id"].items():
                if event_id in existing_ids:
                    duplicate_indices.append(idx)
                else:
                    existing_ids.add(event_id)
            duplicates = valid.loc[duplicate_indices].copy()
            if not duplicates.empty:
                duplicates["reject_reason"] = "duplicate_event_id"
                _append_csv(duplicates, root / "quarantine" / f"{path.stem}_duplicates.csv")
            new_rows = valid.drop(index=duplicate_indices).copy()
            metrics["duplicate_rows"] += len(duplicates)
            metrics["curated_added_rows"] += len(new_rows)
            if not new_rows.empty:
                for (year, month, day), part in new_rows.groupby([
                    new_rows["event_date"].str.slice(0, 4),
                    new_rows["event_date"].str.slice(5, 7),
                    new_rows["event_date"].str.slice(8, 10)
                ]):
                    out = root / "curated" / f"event_year={year}" / f"event_month={month}" / f"event_day={day}" / "orders.csv"
                    _append_csv(part, out)
            manifest["processed_files"][path.name] = digest
            metrics["files_processed"] += 1
            _write_json_atomic(manifest_path, manifest)
    except Exception as exc:
        metrics["status"] = "failed"
        metrics["failure_reason"] = str(exc)
    metrics["ended_at"] = utc_now()
    metrics["elapsed_seconds"] = round(time.perf_counter() - tick, 6)
    ops.mkdir(parents=True, exist_ok=True)
    with (ops / "run_metrics.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(metrics) + "\n")
    reconciliation = {
        "run_id": run_id,
        "input_rows": metrics["input_rows"],
        "valid_rows": metrics["valid_rows"],
        "rejected_rows": metrics["rejected_rows"],
        "duplicate_rows": metrics["duplicate_rows"],
        "curated_added_rows": metrics["curated_added_rows"],
        "row_reconciliation_passed": metrics["input_rows"] == metrics["valid_rows"] + metrics["rejected_rows"],
        "status": metrics["status"],
        "failure_reason": metrics["failure_reason"],
    }
    _write_json_atomic(ops / "reconciliation.json", reconciliation)
    return metrics
