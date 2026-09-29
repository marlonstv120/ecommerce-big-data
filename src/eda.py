"""Reproducible, bounded-memory exploratory analysis for the REES46 event CSVs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pyspark
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType


LOGGER = logging.getLogger("ecommerce_eda")
SOURCE_COLUMNS = (
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)
CORRUPT_COLUMN = "_corrupt_record"
SOURCE_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_COLUMNS]
    + [StructField(CORRUPT_COLUMN, StringType(), True)]
)
EXPECTED_EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")
WEEKDAY_NAMES = {
    1: "domingo",
    2: "lunes",
    3: "martes",
    4: "miércoles",
    5: "jueves",
    6: "viernes",
    7: "sábado",
}
EVENT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss 'UTC'"
LOCAL_FS_JAR_NAME = "hadoop-bare-naked-local-fs-0.1.0.jar"
LOCAL_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
LOCAL_FS_JAR_SHA256 = "e0cc30fb0531eb0b59468dc0abf5b257533d2365b5e9f45e795edd707aa78c62"
LOGICAL_TYPES = {
    "event_time": "timestamp (UTC)",
    "event_type": "categorical string",
    "product_id": "nullable 64-bit integer",
    "category_id": "nullable 64-bit integer",
    "category_code": "nullable hierarchical string",
    "brand": "nullable string",
    "price": "nullable numeric value",
    "user_id": "nullable 64-bit integer",
    "user_session": "nullable session identifier string",
}


def _configure_java23_compatibility() -> None:
    """Apply the Windows/Java 23 workaround with a small local JVM footprint."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    java_options = os.environ.get("JAVA_TOOL_OPTIONS", "").strip()
    for option in (
        "-Djava.security.manager=allow",
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=2",
        "-Xss512k",
    ):
        if option not in java_options:
            java_options = f"{java_options} {option}".strip()
    os.environ["JAVA_TOOL_OPTIONS"] = java_options

    current = os.environ.get("PYSPARK_SUBMIT_ARGS", "").strip()
    if current.endswith("pyspark-shell"):
        current = current[: -len("pyspark-shell")].strip()

    additions = []
    if "--driver-memory" not in current:
        additions.extend(["--driver-memory", "1g"])

    parts = [part for part in (current, " ".join(additions), "pyspark-shell") if part]
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(parts)


def _install_windows_local_fs_adapter() -> None:
    """Add the pinned Java NIO local filesystem adapter to this .venv's Spark jars."""
    project_root = Path(__file__).resolve().parents[1]
    source = project_root / "lib" / LOCAL_FS_JAR_NAME
    if not source.is_file():
        raise FileNotFoundError(f"Falta el adaptador local de Spark: {source}")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    if digest != LOCAL_FS_JAR_SHA256:
        raise ValueError(f"SHA-256 inesperado para {source.name}: {digest}")

    spark_jars = Path(pyspark.__file__).resolve().parent / "jars"
    destination = spark_jars / LOCAL_FS_JAR_NAME
    if not destination.is_file() or hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
        shutil.copy2(source, destination)


def build_spark_session(
    app_name: str = "ecommerce-eda", *, shuffle_partitions: int = 128
) -> SparkSession:
    """Create a modest local Spark session; no external cluster is required."""
    _configure_java23_compatibility()
    if shuffle_partitions < 1:
        raise ValueError("shuffle_partitions debe ser mayor que cero")
    _install_windows_local_fs_adapter()
    session = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.file.impl", LOCAL_FS_CLASS)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(max(2, shuffle_partitions // 16)))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def _validate_csv(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"No existe el CSV de entrada: {resolved}")
    with resolved.open("r", encoding="utf-8-sig", newline="") as stream:
        try:
            header = next(csv.reader(stream))
        except StopIteration as error:
            raise ValueError(f"El CSV está vacío: {resolved}") from error
    if tuple(header) != SOURCE_COLUMNS:
        raise ValueError(
            f"Cabecera inesperada en {resolved.name}: {header!r}; "
            f"se esperaba {list(SOURCE_COLUMNS)!r}"
        )
    return resolved


def _read_events(spark: SparkSession, paths: Sequence[Path]) -> DataFrame:
    raw = (
        spark.read.format("csv")
        .schema(SOURCE_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .option("multiLine", "false")
        .load([str(path) for path in paths])
    )
    frame = raw.withColumn(
        "_source_file",
        F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1),
    )
    frame = frame.withColumn("_event_type_norm", F.trim(F.col("event_type")))
    frame = frame.withColumn(
        "_event_ts",
        F.try_to_timestamp(F.trim(F.col("event_time")), F.lit(EVENT_TIME_FORMAT)),
    )
    for raw_name, parsed_name in (
        ("product_id", "_product_id"),
        ("category_id", "_category_id"),
        ("user_id", "_user_id"),
    ):
        frame = frame.withColumn(
            parsed_name,
            F.expr(f"try_cast(trim(`{raw_name}`) AS BIGINT)"),
        )
    frame = frame.withColumn("_price", F.expr("try_cast(trim(`price`) AS DOUBLE)"))
    return frame


def _missing(column_name: str) -> F.Column:
    value = F.col(column_name)
    return value.isNull() | (F.length(F.trim(value)) == 0)


def _finite_price() -> F.Column:
    value = F.col("_price")
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) < F.lit(float("inf")))


def _as_int(value: object) -> int:
    return int(value) if value is not None else 0


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    # PySpark returns naive Python datetimes in the host's local timezone.
    value_utc = value.astimezone(timezone.utc)
    return value_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _session_funnel(frame: DataFrame) -> dict[str, object]:
    events = (
        frame.filter(
            F.col(CORRUPT_COLUMN).isNull()
            & F.col("_event_ts").isNotNull()
            & ~_missing("user_session")
            & F.col("_event_type_norm").isin("view", "cart", "purchase")
        )
        .select(
            F.trim(F.col("user_session")).alias("_session"),
            F.col("_event_ts"),
            F.col("_event_type_norm"),
        )
    )

    session_times = events.groupBy("_session").agg(
        F.collect_list(F.when(F.col("_event_type_norm") == "view", F.col("_event_ts"))).alias(
            "_view_times"
        ),
        F.collect_list(F.when(F.col("_event_type_norm") == "cart", F.col("_event_ts"))).alias(
            "_cart_times"
        ),
        F.collect_list(
            F.when(F.col("_event_type_norm") == "purchase", F.col("_event_ts"))
        ).alias("_purchase_times"),
    )
    views = session_times.withColumn("view_ts", F.array_min("_view_times")).filter(
        F.col("view_ts").isNotNull()
    )
    carts = views.withColumn(
        "cart_ts",
        F.array_min(
            F.filter(F.col("_cart_times"), lambda event_ts: event_ts >= F.col("view_ts"))
        ),
    )
    funnel = (
        carts.withColumn(
            "purchase_ts",
            F.array_min(
                F.filter(
                    F.col("_purchase_times"),
                    lambda event_ts: event_ts >= F.col("cart_ts"),
                )
            ),
        )
        .select("_session", "view_ts", "cart_ts", "purchase_ts")
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        LOGGER.info("Funnel de sesiones: agregando transiciones cronológicas")
        totals = funnel.agg(
            F.count(F.lit(1)).alias("view_sessions"),
            F.sum(F.when(F.col("cart_ts").isNotNull(), 1).otherwise(0)).alias(
                "cart_after_view_sessions"
            ),
            F.sum(F.when(F.col("purchase_ts").isNotNull(), 1).otherwise(0)).alias(
                "purchase_after_cart_sessions"
            ),
        ).first()
        view_sessions = _as_int(totals["view_sessions"])
        cart_sessions = _as_int(totals["cart_after_view_sessions"])
        purchase_sessions = _as_int(totals["purchase_after_cart_sessions"])

        by_hour_rows = (
            funnel.groupBy(F.hour("view_ts").alias("utc_hour"))
            .agg(
                F.count(F.lit(1)).alias("view_sessions"),
                F.sum(F.when(F.col("cart_ts").isNotNull(), 1).otherwise(0)).alias(
                    "cart_sessions"
                ),
                F.sum(F.when(F.col("purchase_ts").isNotNull(), 1).otherwise(0)).alias(
                    "purchase_sessions"
                ),
            )
            .orderBy("utc_hour")
            .collect()
        )
        by_weekday_rows = (
            funnel.groupBy(F.dayofweek("view_ts").alias("spark_weekday_sunday_1"))
            .agg(
                F.count(F.lit(1)).alias("view_sessions"),
                F.sum(F.when(F.col("cart_ts").isNotNull(), 1).otherwise(0)).alias(
                    "cart_sessions"
                ),
                F.sum(F.when(F.col("purchase_ts").isNotNull(), 1).otherwise(0)).alias(
                    "purchase_sessions"
                ),
            )
            .orderBy("spark_weekday_sunday_1")
            .collect()
        )
    finally:
        funnel.unpersist(blocking=True)

    return {
        "denominator_definition": "sesiones con user_session y vista con timestamp válido",
        "view_sessions": view_sessions,
        "cart_after_view_sessions": cart_sessions,
        "purchase_after_cart_sessions": purchase_sessions,
        "view_to_cart_rate": _ratio(cart_sessions, view_sessions),
        "view_to_purchase_rate": _ratio(purchase_sessions, view_sessions),
        "cart_to_purchase_rate": _ratio(purchase_sessions, cart_sessions),
        "by_utc_hour_of_first_view": [
            {
                "utc_hour": int(row["utc_hour"]),
                "view_sessions": _as_int(row["view_sessions"]),
                "cart_sessions": _as_int(row["cart_sessions"]),
                "purchase_sessions": _as_int(row["purchase_sessions"]),
                "view_to_purchase_rate": _ratio(
                    _as_int(row["purchase_sessions"]), _as_int(row["view_sessions"])
                ),
            }
            for row in by_hour_rows
        ],
        "by_spark_weekday_sunday_1": [
            {
                "weekday_number": int(row["spark_weekday_sunday_1"]),
                "weekday_name_es": WEEKDAY_NAMES[int(row["spark_weekday_sunday_1"])],
                "view_sessions": _as_int(row["view_sessions"]),
                "cart_sessions": _as_int(row["cart_sessions"]),
                "purchase_sessions": _as_int(row["purchase_sessions"]),
                "view_to_purchase_rate": _ratio(
                    _as_int(row["purchase_sessions"]), _as_int(row["view_sessions"])
                ),
            }
            for row in by_weekday_rows
        ],
    }


def _event_frequency(frame: DataFrame) -> dict[str, object]:
    valid_events = frame.filter(
        F.col(CORRUPT_COLUMN).isNull() & F.col("_event_ts").isNotNull()
    )
    hourly = (
        valid_events.groupBy(F.date_trunc("hour", F.col("_event_ts")).alias("hour_utc"))
        .count()
        .orderBy("hour_utc")
        .collect()
    )
    by_hour_of_day = (
        valid_events.groupBy(F.hour("_event_ts").alias("utc_hour_of_day"))
        .count()
        .orderBy("utc_hour_of_day")
        .collect()
    )
    daily = (
        valid_events.groupBy(F.to_date("_event_ts").alias("date_utc"))
        .count()
        .orderBy("date_utc")
        .collect()
    )
    hourly_counts = [_as_int(row["count"]) for row in hourly]
    return {
        "valid_timestamp_events": sum(hourly_counts),
        "observed_utc_hours": len(hourly_counts),
        "mean_events_per_observed_utc_hour": _as_float(
            sum(hourly_counts) / len(hourly_counts) if hourly_counts else None
        ),
        "max_events_in_observed_utc_hour": max(hourly_counts, default=0),
        "by_utc_hour": [
            {"hour": _format_timestamp(row["hour_utc"]), "events": _as_int(row["count"])}
            for row in hourly
        ],
        "by_hour_of_day_utc": [
            {"hour": int(row["utc_hour_of_day"]), "events": _as_int(row["count"])}
            for row in by_hour_of_day
        ],
        "by_utc_day": [
            {"date": row["date_utc"].isoformat(), "events": _as_int(row["count"])}
            for row in daily
        ],
    }


def _purchase_metrics(
    frame: DataFrame, purchase_event_count: int, estimated_value_sum_price: float | None
) -> dict[str, object]:
    purchases = frame.filter(
        F.col(CORRUPT_COLUMN).isNull() & (F.col("_event_type_norm") == "purchase")
    )
    category_key = F.when(_missing("category_code"), F.lit("[sin categoría]")).otherwise(
        F.trim(F.col("category_code"))
    )
    categories = (
        purchases.withColumn("_category_key", category_key)
        .groupBy("_category_key")
        .agg(
            F.count(F.lit(1)).alias("purchase_event_count"),
            F.sum(F.when(_finite_price(), F.col("_price"))).alias("estimated_value_sum_price"),
            F.sum(F.when(~_finite_price(), 1).otherwise(0)).alias(
                "purchase_events_without_valid_price"
            ),
        )
        .orderBy(F.desc("purchase_event_count"), F.asc("_category_key"))
        .limit(20)
        .collect()
    )
    by_hour = (
        purchases.filter(F.col("_event_ts").isNotNull())
        .groupBy(F.hour("_event_ts").alias("utc_hour"))
        .count()
        .orderBy("utc_hour")
        .collect()
    )
    by_weekday = (
        purchases.filter(F.col("_event_ts").isNotNull())
        .groupBy(F.dayofweek("_event_ts").alias("spark_weekday_sunday_1"))
        .count()
        .orderBy("spark_weekday_sunday_1")
        .collect()
    )
    return {
        "purchase_event_count": purchase_event_count,
        "estimated_value_sum_price": estimated_value_sum_price,
        "value_interpretation": "suma de price en eventos purchase válidos; no representa margen ni ventas netas",
        "top_categories": [
            {
                "category_code": row["_category_key"],
                "purchase_event_count": _as_int(row["purchase_event_count"]),
                "estimated_value_sum_price": _as_float(row["estimated_value_sum_price"]),
                "purchase_events_without_valid_price": _as_int(
                    row["purchase_events_without_valid_price"]
                ),
            }
            for row in categories
        ],
        "purchase_events_by_utc_hour_of_day": [
            {"hour": int(row["utc_hour"]), "events": _as_int(row["count"])}
            for row in by_hour
        ],
        "purchase_events_by_spark_weekday_sunday_1": [
            {
                "weekday_number": int(row["spark_weekday_sunday_1"]),
                "weekday_name_es": WEEKDAY_NAMES[int(row["spark_weekday_sunday_1"])],
                "events": _as_int(row["count"]),
            }
            for row in by_weekday
        ],
    }


def _analyze_dataframe(
    frame: DataFrame,
    file_name: str,
    file_bytes: int,
    record_count: int | None = None,
) -> dict[str, object]:
    if record_count is None:
        record_count = frame.count()
    LOGGER.info("%s: calculando calidad y dominios", file_name)
    quality_expressions = []
    for name in SOURCE_COLUMNS:
        quality_expressions.append(
            F.sum(F.when(_missing(name), 1).otherwise(0)).alias(f"missing__{name}")
        )
        quality_expressions.append(
            F.sum(
                F.when(
                    ~_missing(name) & (F.col(name) != F.trim(F.col(name))), 1
                ).otherwise(0)
            ).alias(f"whitespace__{name}")
        )
    quality_expressions.extend(
        [
            F.sum(F.when(F.col(CORRUPT_COLUMN).isNotNull(), 1).otherwise(0)).alias(
                "corrupt_record_count"
            ),
            F.sum(
                F.when(
                    ~_missing("event_time") & F.col("_event_ts").isNull(), 1
                ).otherwise(0)
            ).alias("invalid_timestamp_count"),
            F.sum(
                F.when(
                    ~_missing("event_type")
                    & ~F.col("_event_type_norm").isin(*EXPECTED_EVENT_TYPES),
                    1,
                ).otherwise(0)
            ).alias("unexpected_event_count"),
            F.sum(
                F.when(~_missing("product_id") & F.col("_product_id").isNull(), 1).otherwise(0)
            ).alias("invalid_product_id_count"),
            F.sum(
                F.when(~_missing("category_id") & F.col("_category_id").isNull(), 1).otherwise(0)
            ).alias("invalid_category_id_count"),
            F.sum(F.when(~_missing("user_id") & F.col("_user_id").isNull(), 1).otherwise(0)).alias(
                "invalid_user_id_count"
            ),
            F.sum(
                F.when(F.col("_product_id").isNotNull() & (F.col("_product_id") <= 0), 1).otherwise(0)
            ).alias("nonpositive_product_id_count"),
            F.sum(
                F.when(F.col("_category_id").isNotNull() & (F.col("_category_id") <= 0), 1).otherwise(0)
            ).alias("nonpositive_category_id_count"),
            F.sum(
                F.when(F.col("_user_id").isNotNull() & (F.col("_user_id") <= 0), 1).otherwise(0)
            ).alias("nonpositive_user_id_count"),
            F.sum(
                F.when(~_missing("price") & ~_finite_price(), 1).otherwise(0)
            ).alias("invalid_price_count"),
            F.sum(F.when(_finite_price() & (F.col("_price") <= 0), 1).otherwise(0)).alias(
                "nonpositive_price_count"
            ),
            F.sum(F.when(F.col(CORRUPT_COLUMN).isNull() & _finite_price(), 1).otherwise(0)).alias(
                "valid_price_count"
            ),
            F.sum(
                F.when(
                    F.col(CORRUPT_COLUMN).isNull()
                    & (F.col("_event_type_norm") == "purchase")
                    & _finite_price(),
                    F.col("_price"),
                )
            ).alias("purchase_price_sum"),
            F.sum(
                F.when(
                    F.col(CORRUPT_COLUMN).isNull()
                    & (F.col("_event_type_norm") == "purchase"),
                    1,
                ).otherwise(0)
            ).alias("purchase_event_count"),
            F.sum(F.when(~_missing("user_session"), 1).otherwise(0)).alias(
                "rows_with_session_id"
            ),
            F.min(F.when(F.col(CORRUPT_COLUMN).isNull(), F.col("_event_ts"))).alias(
                "min_event_ts"
            ),
            F.max(F.when(F.col(CORRUPT_COLUMN).isNull(), F.col("_event_ts"))).alias(
                "max_event_ts"
            ),
        ]
    )
    quality = frame.agg(*quality_expressions).first().asDict()
    null_counts = {name: _as_int(quality.get(f"missing__{name}")) for name in SOURCE_COLUMNS}
    whitespace_counts = {
        name: _as_int(quality.get(f"whitespace__{name}")) for name in SOURCE_COLUMNS
    }

    clean = frame.filter(F.col(CORRUPT_COLUMN).isNull())
    event_rows = (
        clean.filter(~_missing("event_type"))
        .groupBy("_event_type_norm")
        .count()
        .orderBy("_event_type_norm")
        .collect()
    )
    event_counts = {row["_event_type_norm"]: _as_int(row["count"]) for row in event_rows}

    LOGGER.info("%s: buscando duplicados por huella SHA-256", file_name)
    canonical_row = F.to_json(
        F.struct(*[F.col(name) for name in (*SOURCE_COLUMNS, CORRUPT_COLUMN)]),
        {"ignoreNullFields": "false"},
    )
    duplicate_rows = (
        frame.select(F.sha2(canonical_row, 256).alias("_row_fingerprint"))
        .groupBy("_row_fingerprint")
        .count()
        .filter(F.col("count") > 1)
        .agg(
            F.sum(F.col("count") - 1).alias("duplicate_excess_rows"),
            F.count(F.lit(1)).alias("duplicate_groups"),
        )
        .first()
    )
    LOGGER.info("%s: calculando cuantiles y valores extremos", file_name)
    valid_prices = frame.filter(F.col(CORRUPT_COLUMN).isNull() & _finite_price())
    quantiles = valid_prices.stat.approxQuantile("_price", [0.0, 0.25, 0.5, 0.75, 1.0], 0.01)
    price_summary: dict[str, object] = {
        "valid_numeric_count": _as_int(quality.get("valid_price_count")),
        "invalid_count": _as_int(quality.get("invalid_price_count")),
        "nonpositive_count": _as_int(quality.get("nonpositive_price_count")),
        "quantile_relative_error": 0.01,
    }
    outlier_count = 0
    if len(quantiles) == 5:
        minimum, q1, median, q3, maximum = quantiles
        iqr = q3 - q1
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
        outlier_count = valid_prices.filter(
            (F.col("_price") < lower_fence) | (F.col("_price") > upper_fence)
        ).count()
        price_summary.update(
            {
                "min": _as_float(minimum),
                "q1": _as_float(q1),
                "median": _as_float(median),
                "q3": _as_float(q3),
                "max": _as_float(maximum),
                "iqr_lower_fence": _as_float(lower_fence),
                "iqr_upper_fence": _as_float(upper_fence),
                "iqr_outlier_candidate_count": outlier_count,
                "outliers_removed": False,
            }
        )
    else:
        price_summary.update(
            {"iqr_outlier_candidate_count": 0, "outliers_removed": False}
        )

    session_row_coverage = _ratio(_as_int(quality.get("rows_with_session_id")), record_count)
    columns = [
        {
            "name": name,
            "csv_storage_type": "string",
            "analysis_type": LOGICAL_TYPES[name],
        }
        for name in SOURCE_COLUMNS
    ]
    min_timestamp = quality.get("min_event_ts")
    max_timestamp = quality.get("max_event_ts")
    LOGGER.info("%s: calculando frecuencias, funnel y compras", file_name)
    return {
        "file": file_name,
        "file_bytes": int(file_bytes),
        "record_count": int(record_count),
        "columns": columns,
        "expected_event_types": list(EXPECTED_EVENT_TYPES),
        "event_counts": event_counts,
        "unexpected_event_count": _as_int(quality.get("unexpected_event_count")),
        "period_utc": {
            "min": _format_timestamp(min_timestamp),
            "max": _format_timestamp(max_timestamp),
        },
        "invalid_timestamp_count": _as_int(quality.get("invalid_timestamp_count")),
        "corrupt_record_count": _as_int(quality.get("corrupt_record_count")),
        "null_counts": null_counts,
        "whitespace_inconsistency_counts": whitespace_counts,
        "invalid_numeric_counts": {
            "product_id": _as_int(quality.get("invalid_product_id_count")),
            "category_id": _as_int(quality.get("invalid_category_id_count")),
            "user_id": _as_int(quality.get("invalid_user_id_count")),
            "price": _as_int(quality.get("invalid_price_count")),
        },
        "nonpositive_id_counts": {
            "product_id": _as_int(quality.get("nonpositive_product_id_count")),
            "category_id": _as_int(quality.get("nonpositive_category_id_count")),
            "user_id": _as_int(quality.get("nonpositive_user_id_count")),
        },
        "duplicate_groups": _as_int(duplicate_rows["duplicate_groups"]),
        "duplicate_excess_rows": _as_int(duplicate_rows["duplicate_excess_rows"]),
        "duplicate_method": "SHA-256 de serialización canónica de las nueve columnas y el campo corrupto; colisiones criptográficas son despreciables",
        "session_row_coverage": {
            "rows_with_nonblank_user_session": _as_int(quality.get("rows_with_session_id")),
            "fraction_of_all_rows": session_row_coverage,
        },
        "price": price_summary,
        "frequency": _event_frequency(clean),
        "funnel": _session_funnel(frame),
        "purchases": _purchase_metrics(
            frame,
            _as_int(quality.get("purchase_event_count")),
            _as_float(quality.get("purchase_price_sum")),
        ),
    }


def analyze_file(spark: SparkSession, path: Path) -> dict[str, object]:
    """Analyze one CSV without collecting event-level data into Python."""
    resolved = _validate_csv(Path(path))
    frame = _read_events(spark, [resolved]).persist(StorageLevel.DISK_ONLY)
    try:
        record_count = frame.count()  # Materialize the disk-backed cache before quality queries.
        return _analyze_dataframe(
            frame, resolved.name, resolved.stat().st_size, record_count=record_count
        )
    finally:
        frame.unpersist(blocking=True)


def analyze_files(
    paths: Sequence[Path], output_path: Path, spark: SparkSession
) -> dict[str, object]:
    """Analyze monthly files and their combined data; write a compact JSON summary."""
    resolved_paths = [_validate_csv(Path(path)) for path in paths]
    if not resolved_paths:
        raise ValueError("Debe indicarse al menos un CSV de entrada")
    if len({path.name for path in resolved_paths}) != len(resolved_paths):
        raise ValueError("Los nombres base de los CSV de entrada deben ser únicos")

    frame = _read_events(spark, resolved_paths).persist(StorageLevel.DISK_ONLY)
    try:
        LOGGER.info("Spark: leyendo y almacenando en disco las filas de entrada")
        record_count = frame.count()
        LOGGER.info("Spark: registros leídos=%s; preparando resumen mensual", record_count)
        overview_expressions = [
            F.count(F.lit(1)).alias("record_count"),
            F.min(F.when(F.col(CORRUPT_COLUMN).isNull(), F.col("_event_ts"))).alias(
                "min_event_ts"
            ),
            F.max(F.when(F.col(CORRUPT_COLUMN).isNull(), F.col("_event_ts"))).alias(
                "max_event_ts"
            ),
            F.sum(F.when(F.col(CORRUPT_COLUMN).isNotNull(), 1).otherwise(0)).alias(
                "corrupt_record_count"
            ),
            F.sum(
                F.when(~_missing("event_time") & F.col("_event_ts").isNull(), 1).otherwise(0)
            ).alias("invalid_timestamp_count"),
        ]
        overview_expressions.extend(
            F.sum(F.when(_missing(name), 1).otherwise(0)).alias(f"missing__{name}")
            for name in SOURCE_COLUMNS
        )
        overview_rows = (
            frame.groupBy("_source_file")
            .agg(*overview_expressions)
            .collect()
        )
        event_rows = (
            frame.filter(F.col(CORRUPT_COLUMN).isNull() & ~_missing("event_type"))
            .groupBy("_source_file", "_event_type_norm")
            .count()
            .collect()
        )
        event_counts_by_file: dict[str, dict[str, int]] = {}
        for row in event_rows:
            event_counts_by_file.setdefault(row["_source_file"], {})[
                row["_event_type_norm"]
            ] = _as_int(row["count"])
        bytes_by_file = {path.name: path.stat().st_size for path in resolved_paths}
        per_file = []
        for row in overview_rows:
            file_name = row["_source_file"]
            per_file.append(
                {
                    "file": file_name,
                    "file_bytes": int(bytes_by_file[file_name]),
                    "record_count": _as_int(row["record_count"]),
                    "period_utc": {
                        "min": _format_timestamp(row["min_event_ts"]),
                        "max": _format_timestamp(row["max_event_ts"]),
                    },
                    "event_counts": event_counts_by_file.get(file_name, {}),
                    "invalid_timestamp_count": _as_int(row["invalid_timestamp_count"]),
                    "corrupt_record_count": _as_int(row["corrupt_record_count"]),
                    "null_counts": {
                        name: _as_int(row[f"missing__{name}"]) for name in SOURCE_COLUMNS
                    },
                }
            )
        LOGGER.info("Spark: iniciando EDA detallado consolidado")
        combined = _analyze_dataframe(
            frame,
            "octubre_noviembre_2019" if len(resolved_paths) == 2 else "consolidado",
            sum(path.stat().st_size for path in resolved_paths),
            record_count=record_count,
        )
    finally:
        frame.unpersist(blocking=True)

    result: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "engine": {"name": "PySpark", "version": spark.version, "mode": "local[2]"},
        "timezone": "UTC",
        "input_files": [path.name for path in resolved_paths],
        "analysis_notes": [
            "Los CSV se procesan con Spark; no se cargan completos en Pandas.",
            "Los eventos purchase suman price como aproximación; no equivale a ingresos netos ni margen.",
            "El detalle de calidad y métricas corresponde al consolidado; cada archivo incluye volumen, periodo, eventos y faltantes.",
            "Los registros originales no se limpian ni se sobrescriben.",
        ],
        "files": per_file,
        "combined": combined,
    }

    destination = Path(output_path).expanduser().resolve()
    if "raw" in {part.casefold() for part in destination.parts}:
        raise ValueError("El resumen no puede guardarse dentro de data/raw")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    temporary.replace(destination)
    LOGGER.info("Resumen guardado en %s", destination)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="EDA de los CSV REES46 con PySpark local, sin carga completa en Pandas."
    )
    parser.add_argument("--input", nargs="+", required=True, type=Path, help="CSV mensuales")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/output/eda_summary.json"),
        help="Ruta para el resumen JSON compacto",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = build_spark_session()
    try:
        analyze_files(args.input, args.output, spark)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
