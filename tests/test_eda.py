import csv
import tempfile
import unittest
from pathlib import Path

from src.eda import analyze_file, build_spark_session


HEADER = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


class EdaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark_session("eda-unit-tests", shuffle_partitions=2)
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="ecommerce-eda-test-")
        cls.root = Path(cls.temp_dir.name)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp_dir.cleanup()

    def write_csv(self, name, rows):
        path = self.root / name
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(HEADER)
            writer.writerows(rows)
        return path

    def analyze(self, name, rows):
        return analyze_file(self.spark, self.write_csv(name, rows))

    def test_record_count_and_semantic_types(self):
        rows = [
            ["2019-10-01 10:00:00 UTC", "view", "1", "10", "category.one", "brand_a", "10.00", "101", "session-1"],
            ["2019-10-01 10:01:00 UTC", "cart", "1", "10", "category.one", "brand_a", "10.00", "101", "session-1"],
            ["2019-10-01 10:02:00 UTC", "purchase", "1", "10", "category.one", "brand_a", "10.00", "101", "session-1"],
        ]
        result = self.analyze("valid.csv", rows)
        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["period_utc"]["min"], "2019-10-01T10:00:00Z")
        self.assertEqual(result["period_utc"]["max"], "2019-10-01T10:02:00Z")
        self.assertEqual(result["event_counts"]["purchase"], 1)

    def test_blank_values_are_counted_as_missing(self):
        rows = [
            ["2019-10-01 10:00:00 UTC", "view", "1", "10", "", "", "10.00", "101", ""],
        ]
        result = self.analyze("blank-values.csv", rows)
        self.assertEqual(result["null_counts"]["category_code"], 1)
        self.assertEqual(result["null_counts"]["brand"], 1)
        self.assertEqual(result["null_counts"]["user_session"], 1)

    def test_whitespace_and_nonpositive_ids_are_reported(self):
        rows = [
            ["2019-10-01 10:00:00 UTC", "view", " -1 ", "10", "category.one", " brand_a ", "10.00", "101", "session-1"],
        ]
        result = self.analyze("inconsistent-format.csv", rows)
        self.assertEqual(result["whitespace_inconsistency_counts"]["product_id"], 1)
        self.assertEqual(result["whitespace_inconsistency_counts"]["brand"], 1)
        self.assertEqual(result["nonpositive_id_counts"]["product_id"], 1)

    def test_out_of_order_events_do_not_convert(self):
        rows = [
            ["2019-10-01 10:00:00 UTC", "view", "1", "10", "category.one", "brand_a", "10.00", "101", "session-order"],
            ["2019-10-01 10:01:00 UTC", "purchase", "1", "10", "category.one", "brand_a", "10.00", "101", "session-order"],
            ["2019-10-01 10:02:00 UTC", "cart", "1", "10", "category.one", "brand_a", "10.00", "101", "session-order"],
        ]
        result = self.analyze("out-of-order.csv", rows)
        self.assertEqual(result["funnel"]["view_sessions"], 1)
        self.assertEqual(result["funnel"]["cart_after_view_sessions"], 1)
        self.assertEqual(result["funnel"]["purchase_after_cart_sessions"], 0)

    def test_duplicate_excess_and_price_issues_are_counted(self):
        duplicate = ["2019-10-01 10:00:00 UTC", "view", "1", "10", "category.one", "brand_a", "-5.00", "101", "session-price"]
        rows = [duplicate, duplicate.copy()]
        rows.append(["2019-10-01 10:01:00 UTC", "view", "2", "10", "category.one", "brand_a", "not-a-price", "101", "session-price"])
        result = self.analyze("duplicates-prices.csv", rows)
        self.assertEqual(result["duplicate_excess_rows"], 1)
        self.assertEqual(result["invalid_numeric_counts"]["price"], 1)
        self.assertEqual(result["price"]["nonpositive_count"], 2)

    def test_invalid_timestamp_and_unexpected_event_are_counted(self):
        rows = [
            ["not-a-timestamp", "search", "1", "10", "category.one", "brand_a", "10.00", "101", "session-invalid"],
        ]
        result = self.analyze("invalid-values.csv", rows)
        self.assertEqual(result["invalid_timestamp_count"], 1)
        self.assertEqual(result["unexpected_event_count"], 1)

    def test_malformed_record_is_reported(self):
        path = self.root / "malformed.csv"
        path.write_text(
            ",".join(HEADER)
            + "\n2019-10-01 10:00:00 UTC,view,1,10,category.one,brand_a,10.00,101,session-ok\n"
            + '2019-10-01 10:01:00 UTC,view,2,10,"unterminated,brand_a,10.00,101,session-bad\n',
            encoding="utf-8",
        )
        result = analyze_file(self.spark, path)
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(result["corrupt_record_count"], 1)


if __name__ == "__main__":
    unittest.main()
