"""Local Spark Batch pipeline for REES46 Bronze → Silver → Gold processing."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.eda import (
    CORRUPT_COLUMN,
    EXPECTED_EVENT_TYPES,
    SOURCE_COLUMNS,
    _finite_price,
    _missing,
    _read_events,
    _validate_csv,
    build_spark_session,
)


LOGGER = logging.getLogger("ecommerce_pipeline")
DUPLICATE_METHOD = (
    "SHA-256 sobre JSON canónico de las nueve columnas; colisiones criptográficas despreciables"
)


def _invalid_row_condition(frame: DataFrame) -> F.Column:
    invalid_numeric = (
        (~_missing("product_id") & F.col("_product_id").isNull())
        | (~_missing("category_id") & F.col("_category_id").isNull())
        | (~_missing("user_id") & F.col("_user_id").isNull())
        | (~_missing("price") & ~_finite_price())
    )
    invalid_event_type = _missing("event_type") | ~F.col("_event_type_norm").isin(
        *EXPECTED_EVENT_TYPES
    )
    return (
        F.col(CORRUPT_COLUMN).isNotNull()
        | _missing("event_time")
        | F.col("_event_ts").isNull()
        | invalid_event_type
        | _missing("product_id")
        | invalid_numeric
    )


def prepare_silver(
    spark: SparkSession, input_paths: Sequence[Path]
) -> tuple[DataFrame, dict[str, object]]:
    """Validate, type, flag and exact-row-deduplicate events for Silver.

    The returned DataFrame is lazy and is written directly by the caller. Raw
    files remain untouched; null category/brand/session values are preserved.
    """
    paths = [_validate_csv(Path(path)) for path in input_paths]
    if not paths:
        raise ValueError("Debe indicarse al menos un CSV de entrada")
    if len({path.name for path in paths}) != len(paths):
        raise ValueError("Los nombres base de los CSV de entrada deben ser únicos")

    raw = _read_events(spark, paths)
    LOGGER.info("Bronze: leyendo %d CSV con Spark y midiendo calidad", len(paths))
    invalid = _invalid_row_condition(raw)
    quality_exprs = [
        F.count(F.lit(1)).alias("source_rows"),
        F.sum(F.when(invalid, 1).otherwise(0)).alias("rejected_rows"),
        F.sum(F.when(F.col(CORRUPT_COLUMN).isNotNull(), 1).otherwise(0)).alias("corrupt_rows"),
        F.sum(F.when(_missing("event_time") | F.col("_event_ts").isNull(), 1).otherwise(0)).alias(
            "invalid_timestamp_rows"
        ),
        F.sum(
            F.when(
                _missing("event_type") | ~F.col("_event_type_norm").isin(*EXPECTED_EVENT_TYPES),
                1,
            ).otherwise(0)
        ).alias("unexpected_event_type_rows"),
        F.sum(F.when(~_finite_price() & ~_missing("price"), 1).otherwise(0)).alias(
            "invalid_price_rows"
        ),
        F.sum(F.when(_finite_price() & (F.col("_price") <= 0), 1).otherwise(0)).alias(
            "nonpositive_price_rows"
        ),
        F.sum(F.when(_missing("category_code"), 1).otherwise(0)).alias(
            "missing_category_code_rows"
        ),
        F.sum(F.when(_missing("brand"), 1).otherwise(0)).alias("missing_brand_rows"),
        F.sum(F.when(_missing("user_session"), 1).otherwise(0)).alias(
            "missing_user_session_rows"
        ),
    ]
    for source_name, parsed_name in (
        ("product_id", "_product_id"),
        ("category_id", "_category_id"),
        ("user_id", "_user_id"),
    ):
        quality_exprs.append(
            F.sum(
                F.when(~_missing(source_name) & F.col(parsed_name).isNull(), 1).otherwise(0)
            ).alias(f"invalid_{source_name}_rows")
        )
    quality_row = raw.agg(*quality_exprs).first().asDict()
    source_rows = int(quality_row["source_rows"] or 0)
    rejected_rows = int(quality_row["rejected_rows"] or 0)
    valid_rows = source_rows - rejected_rows

    valid = raw.filter(~invalid)
    canonical = F.to_json(
        F.struct(*[F.col(name) for name in SOURCE_COLUMNS]),
        {"ignoreNullFields": "false"},
    )
    fingerprinted = valid.withColumn("_row_fingerprint", F.sha2(canonical, 256))
    silver = fingerprinted.dropDuplicates(["_row_fingerprint"]).select(
        F.col("_event_ts").alias("event_time"),
        F.col("_event_type_norm").alias("event_type"),
        F.col("_product_id").alias("product_id"),
        F.col("_category_id").alias("category_id"),
        F.when(_missing("category_code"), F.lit(None).cast("string"))
        .otherwise(F.trim(F.col("category_code")))
        .alias("category_code"),
        F.when(_missing("brand"), F.lit(None).cast("string"))
        .otherwise(F.trim(F.col("brand")))
        .alias("brand"),
        F.col("_price").alias("price"),
        F.col("_user_id").alias("user_id"),
        F.when(_missing("user_session"), F.lit(None).cast("string"))
        .otherwise(F.trim(F.col("user_session")))
        .alias("user_session"),
        F.col("_source_file").alias("source_file"),
        F.date_format(F.col("_event_ts"), "yyyy-MM").alias("event_month"),
        F.when(F.col("_price").isNull(), F.lit("missing"))
        .when(F.col("_price") <= 0, F.lit("nonpositive"))
        .otherwise(F.lit("positive"))
        .alias("price_quality"),
    )
    audit: dict[str, object] = {
        "input_files": [path.name for path in paths],
        "input_file_bytes": {path.name: path.stat().st_size for path in paths},
        "source_rows": source_rows,
        "valid_rows_before_dedup": valid_rows,
        "rejected_rows": rejected_rows,
        "duplicate_method": DUPLICATE_METHOD,
        "quality_counts": {
            "corrupt_rows": int(quality_row.get("corrupt_rows") or 0),
            "invalid_timestamp_rows": int(quality_row.get("invalid_timestamp_rows") or 0),
            "unexpected_event_type_rows": int(
                quality_row.get("unexpected_event_type_rows") or 0
            ),
            "invalid_product_id_rows": int(quality_row.get("invalid_product_id_rows") or 0),
            "invalid_category_id_rows": int(quality_row.get("invalid_category_id_rows") or 0),
            "invalid_user_id_rows": int(quality_row.get("invalid_user_id_rows") or 0),
            "invalid_price_rows": int(quality_row.get("invalid_price_rows") or 0),
            "nonpositive_price_rows": int(quality_row.get("nonpositive_price_rows") or 0),
            "missing_category_code_rows": int(quality_row.get("missing_category_code_rows") or 0),
            "missing_brand_rows": int(quality_row.get("missing_brand_rows") or 0),
            "missing_user_session_rows": int(quality_row.get("missing_user_session_rows") or 0),
        },
    }
    LOGGER.info(
        "Silver DataFrame preparado: source=%s, rechazadas=%s; la deduplicación se materializa al escribir Parquet",
        source_rows,
        rejected_rows,
    )
    return silver, audit


def _funnel_sessions(silver: DataFrame) -> DataFrame:
    events = (
        silver.filter(
            F.col("event_time").isNotNull()
            & F.col("user_session").isNotNull()
            & (F.length(F.trim(F.col("user_session"))) > 0)
            & F.col("event_type").isin("view", "cart", "purchase")
        )
        .select("user_session", "event_time", "event_type")
    )
    by_session = events.groupBy("user_session").agg(
        F.collect_list(F.when(F.col("event_type") == "view", F.col("event_time"))).alias(
            "_views"
        ),
        F.collect_list(F.when(F.col("event_type") == "cart", F.col("event_time"))).alias(
            "_carts"
        ),
        F.collect_list(
            F.when(F.col("event_type") == "purchase", F.col("event_time"))
        ).alias("_purchases"),
    )
    views = by_session.withColumn("view_time", F.array_min("_views")).filter(
        F.col("view_time").isNotNull()
    )
    carts = views.withColumn(
        "cart_time",
        F.array_min(F.filter(F.col("_carts"), lambda event_time: event_time >= F.col("view_time"))),
    )
    return (
        carts.withColumn(
            "purchase_time",
            F.array_min(
                F.filter(
                    F.col("_purchases"),
                    lambda event_time: event_time >= F.col("cart_time"),
                )
            ),
        )
        .select("user_session", "view_time", "cart_time", "purchase_time")
        .withColumn("event_month", F.date_format("view_time", "yyyy-MM"))
    )


def build_gold_metrics(silver: DataFrame) -> dict[str, DataFrame]:
    """Create small Gold aggregates; no event-level rows are collected in Python."""
    funnel_sessions = _funnel_sessions(silver)

    def funnel_aggregate(dimensions: list[str]) -> DataFrame:
        grouped = funnel_sessions.groupBy(*dimensions).agg(
            F.count(F.lit(1)).alias("view_sessions"),
            F.sum(F.when(F.col("cart_time").isNotNull(), 1).otherwise(0)).alias(
                "cart_after_view_sessions"
            ),
            F.sum(F.when(F.col("purchase_time").isNotNull(), 1).otherwise(0)).alias(
                "purchase_after_cart_sessions"
            ),
        )
        return (
            grouped.withColumn(
                "view_to_cart_rate",
                F.col("cart_after_view_sessions") / F.col("view_sessions"),
            )
            .withColumn(
                "view_to_purchase_rate",
                F.col("purchase_after_cart_sessions") / F.col("view_sessions"),
            )
            .withColumn(
                "cart_to_purchase_rate",
                F.when(
                    F.col("cart_after_view_sessions") > 0,
                    F.col("purchase_after_cart_sessions") / F.col("cart_after_view_sessions"),
                ),
            )
        )

    funnel_by_month = funnel_aggregate(["event_month"])
    funnel_by_utc_weekday_hour = funnel_aggregate(
        [
            "event_month",
            F.dayofweek("view_time").alias("spark_weekday_sunday_1"),
            F.hour("view_time").alias("utc_hour"),
        ]
    )

    purchases = silver.filter(F.col("event_type") == "purchase")
    category = F.coalesce(
        F.when(
            F.col("category_code").isNull() | (F.length(F.trim("category_code")) == 0),
            F.lit("[sin categoría]"),
        ).otherwise(F.trim(F.col("category_code"))),
        F.lit("[sin categoría]"),
    )
    purchases_by_category = purchases.withColumn("category_code", category).groupBy(
        "category_code"
    ).agg(
        F.count(F.lit(1)).alias("purchase_event_count"),
        F.sum("price").alias("estimated_value_sum_price"),
        F.sum(F.when(F.col("price_quality") == "nonpositive", 1).otherwise(0)).alias(
            "nonpositive_price_events"
        ),
    )

    purchases_by_category_day = (
        purchases.withColumn("category_code", category)
        .withColumn("purchase_date_utc", F.to_date("event_time"))
        .groupBy("event_month", "purchase_date_utc", "category_code")
        .agg(
            F.count(F.lit(1)).alias("purchase_event_count"),
            F.sum("price").alias("estimated_value_sum_price"),
            F.sum(F.when(F.col("price_quality") == "nonpositive", 1).otherwise(0)).alias(
                "nonpositive_price_events"
            ),
        )
    )

    purchases_by_utc_time = purchases.withColumn(
        "purchase_date_utc", F.to_date("event_time")
    ).withColumn("utc_hour", F.hour("event_time")).groupBy(
        "event_month", "purchase_date_utc", "utc_hour"
    ).agg(
        F.count(F.lit(1)).alias("purchase_event_count"),
        F.sum("price").alias("estimated_value_sum_price"),
    )

    return {
        "funnel_by_month": funnel_by_month,
        "funnel_by_utc_weekday_hour": funnel_by_utc_weekday_hour,
        "purchases_by_category": purchases_by_category,
        "purchases_by_category_day": purchases_by_category_day,
        "purchases_by_utc_time": purchases_by_utc_time,
    }


def _validate_output_path(path: Path, label: str) -> Path:
    destination = path.expanduser().resolve()
    if "raw" in {part.casefold() for part in destination.parts}:
        raise ValueError(f"{label} no puede guardarse dentro de data/raw")
    return destination


def run_pipeline(
    spark: SparkSession,
    input_paths: Sequence[Path],
    silver_path: Path,
    gold_path: Path,
    summary_path: Path,
) -> dict[str, object]:
    """Write Silver and Gold Parquet plus a compact, JSON-serializable audit."""
    silver_destination = _validate_output_path(Path(silver_path), "Silver")
    gold_destination = _validate_output_path(Path(gold_path), "Gold")
    summary_destination = _validate_output_path(Path(summary_path), "El resumen")
    silver, audit = prepare_silver(spark, input_paths)
    LOGGER.info("Silver: escribiendo Parquet particionado por event_month")
    silver.write.mode("overwrite").partitionBy("event_month").parquet(
        str(silver_destination)
    )
    silver_readback = spark.read.parquet(str(silver_destination))
    silver_readback_rows = silver_readback.count()
    audit["silver_rows"] = silver_readback_rows
    audit["duplicate_excess_rows_removed"] = (
        int(audit["valid_rows_before_dedup"]) - silver_readback_rows
    )

    metric_frames = build_gold_metrics(silver_readback)
    gold_row_counts: dict[str, int] = {}
    gold_outputs: dict[str, str] = {}
    for name, metric_frame in metric_frames.items():
        destination = gold_destination / name
        LOGGER.info("Gold: escribiendo %s", name)
        metric_frame.coalesce(1).write.mode("overwrite").parquet(str(destination))
        gold_row_counts[name] = spark.read.parquet(str(destination)).count()
        gold_outputs[name] = str(destination)

    result: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "engine": {"name": "PySpark", "version": spark.version, "mode": "local[2]"},
        "storage_format": "Parquet",
        "bronze": {"input_files": audit["input_files"], "raw_modified": False},
        "silver": {
            "path": str(silver_destination),
            "rows": int(audit["silver_rows"]),
            "partition_column": "event_month",
        },
        "gold": {"path": str(gold_destination), "outputs": gold_outputs, "row_counts": gold_row_counts},
        **audit,
    }
    summary_destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = summary_destination.with_name(summary_destination.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    temporary.replace(summary_destination)
    LOGGER.info("Resumen del pipeline guardado en %s", summary_destination)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Pipeline local Batch REES46: Bronze CSV → Silver/Gold Parquet."
    )
    parser.add_argument("--input", nargs="+", required=True, type=Path, help="CSV mensuales")
    parser.add_argument(
        "--silver-output",
        type=Path,
        default=Path("data/processed/silver/events"),
    )
    parser.add_argument(
        "--gold-output", type=Path, default=Path("data/processed/gold")
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=Path("data/output/pipeline_summary.json"),
    )
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = build_spark_session("ecommerce-phase2-pipeline", shuffle_partitions=args.shuffle_partitions)
    try:
        run_pipeline(
            spark,
            args.input,
            args.silver_output,
            args.gold_output,
            args.summary_output,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
