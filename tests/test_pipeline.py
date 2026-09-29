import csv
import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql.types import LongType, TimestampType

from src.eda import SOURCE_COLUMNS, build_spark_session
from src.pipeline import build_gold_metrics, prepare_silver, run_pipeline


def sample_rows():
    first_view = [
        "2019-10-01 10:00:00 UTC", "view", "1", "10", "electronics.audio", "brand_a", "10.00", "101", "session-1"
    ]
    return [
        first_view,
        first_view.copy(),  # exact repeat: retained in Bronze, collapsed in Silver
        ["2019-10-01 10:01:00 UTC", "cart", "1", "10", "electronics.audio", "brand_a", "10.00", "101", "session-1"],
        ["2019-10-01 10:02:00 UTC", "purchase", "1", "10", "electronics.audio", "brand_a", "10.00", "101", "session-1"],
        ["2019-10-01 11:00:00 UTC", "view", "2", "20", "", "", "0.00", "102", "session-2"],
        ["2019-10-01 11:01:00 UTC", "purchase", "2", "20", "", "", "0.00", "102", "session-2"],
        ["2019-10-01 12:00:00 UTC", "view", "3", "30", "electronics.video", "brand_c", "7.50", "103", "session-3"],
        ["2019-10-01 12:01:00 UTC", "purchase", "3", "30", "electronics.video", "brand_c", "7.50", "103", "session-3"],
        ["2019-10-01 12:02:00 UTC", "cart", "3", "30", "electronics.video", "brand_c", "7.50", "103", "session-3"],
        ["not-a-timestamp", "view", "4", "40", "electronics.test", "brand_d", "1.00", "104", "session-4"],
    ]


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark_session("phase2-pipeline-tests", shuffle_partitions=2)
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="ecommerce-pipeline-test-")
        cls.root = Path(cls.temp_dir.name)
        cls.input_path = cls.root / "2019-Oct.csv"
        with cls.input_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(SOURCE_COLUMNS)
            writer.writerows(sample_rows())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp_dir.cleanup()

    def test_silver_casts_deduplicates_and_preserves_allowed_nulls(self):
        silver, audit = prepare_silver(self.spark, [self.input_path])
        self.assertEqual(audit["source_rows"], 10)
        self.assertEqual(audit["rejected_rows"], 1)
        self.assertEqual(audit["quality_counts"]["invalid_timestamp_rows"], 1)
        self.assertEqual(audit["valid_rows_before_dedup"], 9)
        silver_rows = silver.count()
        self.assertEqual(audit["valid_rows_before_dedup"] - silver_rows, 1)
        self.assertEqual(silver_rows, 8)
        self.assertIsInstance(silver.schema["event_time"].dataType, TimestampType)
        self.assertIsInstance(silver.schema["product_id"].dataType, LongType)
        self.assertEqual(silver.filter("category_code IS NULL").count(), 2)
        self.assertEqual(silver.filter("price_quality = 'nonpositive'").count(), 2)

    def test_gold_metrics_and_full_pipeline_parquet_outputs(self):
        silver, _ = prepare_silver(self.spark, [self.input_path])
        metrics = build_gold_metrics(silver)
        funnel = metrics["funnel_by_month"].first().asDict()
        self.assertEqual(funnel["view_sessions"], 3)
        self.assertEqual(funnel["cart_after_view_sessions"], 2)
        self.assertEqual(funnel["purchase_after_cart_sessions"], 1)

        categories = {
            row["category_code"]: row.asDict()
            for row in metrics["purchases_by_category"].collect()
        }
        self.assertEqual(categories["electronics.audio"]["purchase_event_count"], 1)
        self.assertEqual(categories["[sin categoría]"]["purchase_event_count"], 1)
        self.assertEqual(categories["[sin categoría]"]["estimated_value_sum_price"], 0.0)
        self.assertEqual(metrics["purchases_by_category_day"].count(), 3)

        result = run_pipeline(
            self.spark,
            [self.input_path],
            self.root / "processed" / "silver" / "events",
            self.root / "processed" / "gold",
            self.root / "output" / "pipeline_summary.json",
        )
        self.assertEqual(result["silver_rows"], 8)
        self.assertEqual(result["gold"]["row_counts"]["funnel_by_month"], 1)
        self.assertEqual(result["gold"]["row_counts"]["purchases_by_category_day"], 3)
        self.assertEqual(
            self.spark.read.parquet(str(self.root / "processed" / "silver" / "events")).count(),
            8,
        )
        self.assertTrue((self.root / "processed" / "gold" / "purchases_by_category").is_dir())
        saved = json.loads((self.root / "output" / "pipeline_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["duplicate_excess_rows_removed"], 1)


if __name__ == "__main__":
    unittest.main()
