from pathlib import Path
import csv
import random
from datetime import datetime, timedelta, timezone

FIELDS = ["event_id", "customer_id", "event_timestamp", "amount", "currency"]

def generate(rows=100, seed=42, out_dir="data/incoming"):
    """Generate deterministic synthetic data. Truth labels are kept out of input files."""
    if rows < 0:
        raise ValueError("rows must be >= 0")
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    main_path = out / "orders_001.csv"
    truth_path = out.parent / "generator_truth.csv"
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    truth = []
    with main_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for i in range(rows):
            ts = start + timedelta(minutes=rng.randint(0, 60 * 24 * 90))
            record = {
                "event_id": f"EVT{i+1:08d}",
                "customer_id": f"C{rng.randint(1, 10000):05d}",
                "event_timestamp": ts.isoformat().replace("+00:00", "Z"),
                "amount": f"{rng.uniform(1, 5000):.2f}",
                "currency": rng.choice(["USD", "INR", "EUR"]),
            }
            label = "valid"
            # Deterministic, low-rate negative cases for non-trivial batch sizes.
            if i % 97 == 20:
                record["amount"] = "-10.00"; label = "negative_amount"
            elif i % 89 == 15:
                record["customer_id"] = "UNKNOWN"; label = "invalid_customer_reference"
            elif i % 83 == 10:
                record["event_timestamp"] = "not-a-date"; label = "invalid_event_timestamp"
            elif i % 79 == 7:
                record["event_id"] = ""; label = "missing_event_id"
            writer.writerow(record)
            truth.append({"row_number": i + 2, "expected_label": label})
    # Add duplicate delivery in a separate file when at least one row exists.
    if rows:
        duplicate_path = out / "orders_002_duplicate.csv"
        with main_path.open(newline="", encoding="utf-8") as src:
            reader = csv.DictReader(src)
            first = next(reader, None)
        if first:
            with duplicate_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerow(first)
            truth.append({"row_number": 2, "expected_label": "duplicate_delivery_in_orders_002_duplicate.csv"})
    with truth_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["row_number", "expected_label"])
        writer.writeheader()
        writer.writerows(truth)
    return [main_path] + ([out / "orders_002_duplicate.csv"] if rows else [])
