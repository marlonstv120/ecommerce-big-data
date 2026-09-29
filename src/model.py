"""Session-level Spark ML baseline for predicting purchases after an observation window."""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.eda import _missing, build_spark_session


LOGGER = logging.getLogger("ecommerce_model")
DEFAULT_CUTOFF_MINUTES = 5
DEFAULT_SAMPLE_PERCENT = 2
FEATURE_COLUMNS = (
    "log_views_5m",
    "log_carts_5m",
    "log_removals_5m",
    "log_unique_products_5m",
    "log_unique_categories_5m",
    "mean_view_price_5m",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
)


def build_session_examples(
    sampled_events: DataFrame,
    min_event_time: str,
    max_event_time: str,
    cutoff_minutes: int = DEFAULT_CUTOFF_MINUTES,
) -> DataFrame:
    """Build one leakage-safe binary example per session from sampled Silver events.

    Features use only non-purchase events strictly before the fixed cutoff. Sessions
    with a purchase before the cutoff and sessions censored at the date boundaries
    are excluded. The positive label means a purchase occurs at/after the cutoff.
    """
    if cutoff_minutes <= 0:
        raise ValueError("cutoff_minutes debe ser mayor que cero")

    interval = F.expr(f"INTERVAL {int(cutoff_minutes)} MINUTES")
    sessions = (
        sampled_events.filter(~_missing("user_session") & F.col("event_time").isNotNull())
        .groupBy("user_session")
        .agg(
            F.min("event_time").alias("session_start"),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_time"))).alias(
                "first_purchase_time"
            ),
        )
        .withColumn("cutoff_time", F.col("session_start") + interval)
        .withColumn("event_month", F.date_format("session_start", "yyyy-MM"))
    )

    min_timestamp = F.to_timestamp(F.lit(min_event_time))
    max_timestamp = F.to_timestamp(F.lit(max_event_time))
    eligible = sessions.filter(
        (F.col("session_start") >= min_timestamp + interval)
        & (F.col("session_start") <= max_timestamp - interval)
        & (F.date_format("cutoff_time", "yyyy-MM") == F.col("event_month"))
        & (
            F.col("first_purchase_time").isNull()
            | (F.col("first_purchase_time") >= F.col("cutoff_time"))
        )
    ).withColumn(
        "label",
        F.when(F.col("first_purchase_time").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)),
    )

    pre_cutoff = (
        sampled_events.join(
            eligible.select("user_session", "cutoff_time"),
            on="user_session",
            how="inner",
        )
        .filter(
            (F.col("event_time") < F.col("cutoff_time"))
            & (F.col("event_type") != "purchase")
        )
    )
    features = pre_cutoff.groupBy("user_session").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views_5m"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts_5m"),
        F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias(
            "removals_5m"
        ),
        F.countDistinct(
            F.when(F.col("event_type") == "view", F.col("product_id"))
        ).alias("unique_products_5m"),
        F.countDistinct(
            F.when(F.col("event_type") == "view", F.col("category_code"))
        ).alias("unique_categories_5m"),
        F.avg(
            F.when(
                (F.col("event_type") == "view")
                & (F.col("price_quality") == "positive"),
                F.col("price"),
            )
        ).alias("mean_view_price_5m"),
    )

    examples = eligible.join(features, on="user_session", how="left")
    count_features = [
        "views_5m",
        "carts_5m",
        "removals_5m",
        "unique_products_5m",
        "unique_categories_5m",
        "mean_view_price_5m",
    ]
    examples = examples.fillna(0, subset=count_features)
    hour_angle = F.lit(2.0 * math.pi / 24.0) * F.hour("session_start")
    weekday_zero_based = F.dayofweek("session_start") - F.lit(1)
    weekday_angle = F.lit(2.0 * math.pi / 7.0) * weekday_zero_based
    examples = (
        examples.withColumn("log_views_5m", F.log1p(F.col("views_5m")))
        .withColumn("log_carts_5m", F.log1p(F.col("carts_5m")))
        .withColumn("log_removals_5m", F.log1p(F.col("removals_5m")))
        .withColumn("log_unique_products_5m", F.log1p(F.col("unique_products_5m")))
        .withColumn("log_unique_categories_5m", F.log1p(F.col("unique_categories_5m")))
        .withColumn("hour_sin", F.sin(hour_angle))
        .withColumn("hour_cos", F.cos(hour_angle))
        .withColumn("weekday_sin", F.sin(weekday_angle))
        .withColumn("weekday_cos", F.cos(weekday_angle))
    )
    return examples.select(
        "user_session",
        "event_month",
        "session_start",
        "first_purchase_time",
        "label",
        *FEATURE_COLUMNS,
    )


def _class_counts(frame: DataFrame) -> dict[int, int]:
    return {int(row["label"]): int(row["count"]) for row in frame.groupBy("label").count().collect()}


def _confusion_metrics(predictions: DataFrame) -> tuple[dict[str, int], dict[str, float | None]]:
    counts = predictions.agg(
        F.sum(F.when((F.col("label") == 1.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias(
            "true_positive"
        ),
        F.sum(F.when((F.col("label") == 0.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias(
            "false_positive"
        ),
        F.sum(F.when((F.col("label") == 0.0) & (F.col("prediction") == 0.0), 1).otherwise(0)).alias(
            "true_negative"
        ),
        F.sum(F.when((F.col("label") == 1.0) & (F.col("prediction") == 0.0), 1).otherwise(0)).alias(
            "false_negative"
        ),
    ).first()
    matrix = {key: int(counts[key] or 0) for key in (
        "true_positive", "false_positive", "true_negative", "false_negative"
    )}
    tp, fp, tn, fn = (
        matrix["true_positive"],
        matrix["false_positive"],
        matrix["true_negative"],
        matrix["false_negative"],
    )
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = 2 * precision * recall / (precision + recall) if precision and recall else 0.0
    accuracy = (tp + tn) / (tp + fp + tn + fn) if tp + fp + tn + fn else None
    return matrix, {"precision": precision, "recall": recall, "f1": f1, "accuracy": accuracy}


def train_and_evaluate(
    spark: SparkSession,
    silver_path: Path,
    eda_summary_path: Path,
    model_path: Path,
    metrics_path: Path,
    sample_percent: int = DEFAULT_SAMPLE_PERCENT,
    cutoff_minutes: int = DEFAULT_CUTOFF_MINUTES,
) -> dict[str, object]:
    """Train a weighted logistic regression on October and evaluate on November."""
    if not 1 <= sample_percent <= 100:
        raise ValueError("sample_percent debe estar entre 1 y 100")
    eda_summary = json.loads(Path(eda_summary_path).read_text(encoding="utf-8"))
    period = eda_summary["combined"]["period_utc"]

    silver = spark.read.parquet(str(silver_path))
    sampled_events = (
        silver.filter(~_missing("user_session"))
        .filter(F.pmod(F.xxhash64(F.col("user_session")), F.lit(100)) < sample_percent)
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        sampled_event_rows = sampled_events.count()
        LOGGER.info("ML: eventos de las sesiones muestreadas=%s", sampled_event_rows)
        examples = build_session_examples(
            sampled_events,
            period["min"],
            period["max"],
            cutoff_minutes=cutoff_minutes,
        ).persist(StorageLevel.DISK_ONLY)
        try:
            example_count = examples.count()
            train = examples.filter(F.col("event_month") == "2019-10")
            test = examples.filter(F.col("event_month") == "2019-11")
            train_class_counts = _class_counts(train)
            test_class_counts = _class_counts(test)
            if train_class_counts.get(0, 0) == 0 or train_class_counts.get(1, 0) == 0:
                raise ValueError(f"La muestra de entrenamiento no contiene ambas clases: {train_class_counts}")
            if test_class_counts.get(0, 0) == 0 or test_class_counts.get(1, 0) == 0:
                raise ValueError(f"La muestra de prueba no contiene ambas clases: {test_class_counts}")

            train_total = sum(train_class_counts.values())
            negative_weight = train_total / (2.0 * train_class_counts[0])
            positive_weight = train_total / (2.0 * train_class_counts[1])
            assembler = VectorAssembler(
                inputCols=list(FEATURE_COLUMNS),
                outputCol="features",
                handleInvalid="error",
            )
            weighted_train = assembler.transform(train).withColumn(
                "_class_weight",
                F.when(F.col("label") == 1.0, F.lit(positive_weight)).otherwise(
                    F.lit(negative_weight)
                ),
            )
            prepared_test = assembler.transform(test)
            estimator = LogisticRegression(
                labelCol="label",
                featuresCol="features",
                weightCol="_class_weight",
                maxIter=40,
                regParam=0.05,
                elasticNetParam=0.0,
                threshold=0.5,
            )
            LOGGER.info(
                "ML: entrenando octubre n=%s clases=%s; evaluación temporal noviembre n=%s clases=%s",
                train_total,
                train_class_counts,
                sum(test_class_counts.values()),
                test_class_counts,
            )
            model = estimator.fit(weighted_train)
            predictions = model.transform(prepared_test).withColumn(
                "_positive_probability", vector_to_array(F.col("probability"))[1]
            )
            matrix, rates = _confusion_metrics(predictions)
            roc_auc = BinaryClassificationEvaluator(
                labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
            ).evaluate(predictions)
            pr_auc = BinaryClassificationEvaluator(
                labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
            ).evaluate(predictions)
            score_rows = (
                predictions.withColumn(
                    "probability_decile",
                    F.least(F.lit(9), F.floor(F.col("_positive_probability") * 10)).cast("int"),
                )
                .groupBy("probability_decile", "label")
                .count()
                .orderBy("probability_decile", "label")
                .collect()
            )
            score_deciles = [
                {
                    "decile": int(row["probability_decile"]),
                    "actual_label": int(row["label"]),
                    "session_count": int(row["count"]),
                }
                for row in score_rows
            ]
            coefficients = [float(value) for value in model.coefficients.toArray().tolist()]

            destination_model = Path(model_path).expanduser().resolve()
            if "raw" in {part.casefold() for part in destination_model.parts}:
                raise ValueError("El modelo no puede guardarse dentro de data/raw")
            model.write().overwrite().save(str(destination_model))

            result: dict[str, object] = {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "engine": {"name": "Spark MLlib", "spark_version": spark.version},
                "model": "weighted logistic regression",
                "model_path": str(destination_model),
                "target_definition": (
                    f"compra con event_time >= session_start + {cutoff_minutes} minutos; "
                    "sesiones con compra previa al corte excluidas"
                ),
                "features_definition": (
                    f"actividad no-purchase observada estrictamente antes de los primeros {cutoff_minutes} minutos; "
                    "sin user_id/product_id como identificador predictivo"
                ),
                "sampling": {
                    "deterministic_session_hash_percent": sample_percent,
                    "sampled_event_rows": sampled_event_rows,
                    "eligible_session_examples": example_count,
                },
                "temporal_split": {
                    "train_month": "2019-10",
                    "test_month": "2019-11",
                    "train_class_counts": train_class_counts,
                    "test_class_counts": test_class_counts,
                },
                "class_weights": {"0": negative_weight, "1": positive_weight},
                "decision_threshold": 0.5,
                "metrics": {
                    **rates,
                    "roc_auc": float(roc_auc),
                    "pr_auc": float(pr_auc),
                    "confusion_matrix": matrix,
                },
                "score_deciles": score_deciles,
                "feature_coefficients": [
                    {"feature": name, "coefficient": coefficient}
                    for name, coefficient in zip(FEATURE_COLUMNS, coefficients)
                ],
                "limitations": [
                    "Las métricas evalúan una muestra hash determinista del 2 % por defecto.",
                    "La prueba temporal usa octubre para entrenamiento y noviembre para test.",
                    "La predicción se refiere a compras posteriores a la ventana de observación de cinco minutos.",
                    "Una probabilidad no demuestra causalidad ni garantiza una compra.",
                ],
            }
            metrics_destination = Path(metrics_path).expanduser().resolve()
            if "raw" in {part.casefold() for part in metrics_destination.parts}:
                raise ValueError("Las métricas no pueden guardarse dentro de data/raw")
            metrics_destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = metrics_destination.with_name(metrics_destination.name + ".tmp")
            with temporary.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
                stream.write("\n")
            temporary.replace(metrics_destination)
            LOGGER.info("ML: evaluación guardada en %s", metrics_destination)
            return result
        finally:
            examples.unpersist(blocking=True)
    finally:
        sampled_events.unpersist(blocking=True)


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Entrena/evalúa un modelo Spark MLlib de compra posterior a una ventana de sesión."
    )
    parser.add_argument(
        "--silver-input", type=Path, default=Path("data/processed/silver/events")
    )
    parser.add_argument(
        "--eda-summary", type=Path, default=Path("data/output/eda_summary.json")
    )
    parser.add_argument(
        "--model-output",
        type=Path,
        default=Path("data/processed/models/session_purchase_after_5m"),
    )
    parser.add_argument(
        "--metrics-output",
        type=Path,
        default=Path("data/output/model_evaluation.json"),
    )
    parser.add_argument("--sample-percent", type=int, default=DEFAULT_SAMPLE_PERCENT)
    parser.add_argument("--cutoff-minutes", type=int, default=DEFAULT_CUTOFF_MINUTES)
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = build_spark_session("ecommerce-session-purchase-model", shuffle_partitions=args.shuffle_partitions)
    try:
        train_and_evaluate(
            spark,
            args.silver_input,
            args.eda_summary,
            args.model_output,
            args.metrics_output,
            sample_percent=args.sample_percent,
            cutoff_minutes=args.cutoff_minutes,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
