import csv
import tempfile
import unittest
from pathlib import Path
import pandas as pd
from fileflow.pipeline import run_pipeline, validate_frame, EXPECTED

def row(event_id="E1", customer_id="C00001", timestamp="2026-01-02T03:04:05Z", amount="10.5", currency="USD"):
    return {"event_id": event_id, "customer_id": customer_id, "event_timestamp": timestamp, "amount": amount, "currency": currency}

class PipelineTests(unittest.TestCase):
    def test_happy_path_and_partition(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"; inc = root / "incoming"; inc.mkdir(parents=True)
            pd.DataFrame([row()]).to_csv(inc / "a.csv", index=False)
            result = run_pipeline(inc, root)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["curated_added_rows"], 1)
            self.assertTrue(list((root / "curated").rglob("orders.csv")))

    def test_empty_batch(self):
        frame = pd.DataFrame(columns=EXPECTED)
        valid, rejected = validate_frame(frame)
        self.assertEqual(len(valid), 0); self.assertEqual(len(rejected), 0)

    def test_missing_required_value(self):
        valid, rejected = validate_frame(pd.DataFrame([row(event_id="")]))
        self.assertEqual(len(valid), 0)
        self.assertIn("missing_event_id", rejected.iloc[0]["reject_reason"])

    def test_negative_amount_and_bad_reference(self):
        valid, rejected = validate_frame(pd.DataFrame([row(event_id="E1", amount="-2"), row(event_id="E2", customer_id="BAD")]))
        self.assertEqual(len(valid), 0)
        self.assertIn("negative_amount", rejected.iloc[0]["reject_reason"])
        self.assertIn("invalid_customer_reference", rejected.iloc[1]["reject_reason"])

    def test_invalid_timestamp(self):
        valid, rejected = validate_frame(pd.DataFrame([row(timestamp="not-a-date")]))
        self.assertEqual(len(valid), 0)
        self.assertIn("invalid_event_timestamp", rejected.iloc[0]["reject_reason"])

    def test_schema_change_fails(self):
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            validate_frame(pd.DataFrame([{"event_id": "E1", "unexpected": "x"}]))

    def test_duplicate_event_in_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"; inc = root / "incoming"; inc.mkdir(parents=True)
            pd.DataFrame([row(), row()]).to_csv(inc / "a.csv", index=False)
            result = run_pipeline(inc, root)
            self.assertEqual(result["curated_added_rows"], 1)
            self.assertEqual(result["duplicate_rows"], 1)

    def test_rerun_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"; inc = root / "incoming"; inc.mkdir(parents=True)
            pd.DataFrame([row()]).to_csv(inc / "a.csv", index=False)
            first = run_pipeline(inc, root)
            second = run_pipeline(inc, root)
            self.assertEqual(first["curated_added_rows"], 1)
            self.assertEqual(second["curated_added_rows"], 0)
            self.assertEqual(second["files_skipped"], 1)

    def test_late_arrival_uses_event_date_partition(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"; inc = root / "incoming"; inc.mkdir(parents=True)
            pd.DataFrame([row(timestamp="2024-02-03T00:00:00Z")]).to_csv(inc / "late.csv", index=False)
            run_pipeline(inc, root)
            self.assertTrue((root / "curated/event_year=2024/event_month=02/event_day=03/orders.csv").exists())

if __name__ == "__main__":
    unittest.main()
