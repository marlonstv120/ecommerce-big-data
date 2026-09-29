import math
import unittest
from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.eda import build_spark_session
from src.model import build_session_examples


class ModelFeatureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark_session("session-model-feature-tests", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_features_stop_at_cutoff_and_target_uses_later_purchase(self):
        schema = StructType(
            [
                StructField("user_session", StringType(), False),
                StructField("event_time", TimestampType(), False),
                StructField("event_type", StringType(), False),
                StructField("product_id", LongType(), True),
                StructField("category_code", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("price_quality", StringType(), True),
                StructField("event_month", StringType(), False),
            ]
        )
        rows = [
            ("late-buy", datetime(2019, 10, 10, 10, 0), "view", 1, "cat.a", 20.0, "positive", "2019-10"),
            ("late-buy", datetime(2019, 10, 10, 10, 2), "cart", 1, "cat.a", 20.0, "positive", "2019-10"),
            ("late-buy", datetime(2019, 10, 10, 10, 6), "purchase", 1, "cat.a", 20.0, "positive", "2019-10"),
            ("no-buy", datetime(2019, 10, 10, 11, 0), "view", 2, "cat.b", 30.0, "positive", "2019-10"),
            ("no-buy", datetime(2019, 10, 10, 11, 2), "cart", 2, "cat.b", 30.0, "positive", "2019-10"),
            ("early-buy", datetime(2019, 10, 10, 12, 0), "view", 3, "cat.c", 40.0, "positive", "2019-10"),
            ("early-buy", datetime(2019, 10, 10, 12, 3), "purchase", 3, "cat.c", 40.0, "positive", "2019-10"),
        ]
        events = self.spark.createDataFrame(rows, schema=schema)
        examples = build_session_examples(
            events,
            "2019-10-01T00:00:00Z",
            "2019-11-30T23:59:59Z",
            cutoff_minutes=5,
        )
        result = {row["user_session"]: row.asDict() for row in examples.collect()}
        self.assertEqual(set(result), {"late-buy", "no-buy"})
        self.assertEqual(result["late-buy"]["label"], 1.0)
        self.assertEqual(result["no-buy"]["label"], 0.0)
        self.assertEqual(result["late-buy"]["log_views_5m"], math.log(2.0))
        self.assertEqual(result["late-buy"]["log_carts_5m"], math.log(2.0))
        self.assertAlmostEqual(result["late-buy"]["mean_view_price_5m"], 20.0)
        # The purchase at 10:06 is the label, not a feature from the observation window.
        self.assertEqual(result["late-buy"]["log_views_5m"], result["no-buy"]["log_views_5m"])


if __name__ == "__main__":
    unittest.main()
