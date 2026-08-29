from datetime import datetime
from uuid import uuid4

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


QUALITY_LOG_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)


def run_quality_checks(spark, config):
    tenant = config.tenant.code.lower()

    silver_path = (
        f"{config.paths.silver}/"
        f"{tenant}/"
        "fact_deliveries"
    )

    quality_logs_path = config.paths.quality_logs

    df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # ---------------------------------------------------------
    # Apply execution date range
    # ---------------------------------------------------------

    if config.execution.start_date:
        start_date = (
            config.execution.start_date
            .replace("-", "")
        )

        df = df.filter(
            F.col("fecha_proceso") >= start_date
        )

    if config.execution.end_date:
        end_date = (
            config.execution.end_date
            .replace("-", "")
        )

        df = df.filter(
            F.col("fecha_proceso") <= end_date
        )

    records_checked = df.count()

    # ---------------------------------------------------------
    # Technical execution identifiers
    # ---------------------------------------------------------

    start_date_value = (
        config.execution.start_date
        or "all"
    )

    end_date_value = (
        config.execution.end_date
        or "all"
    )

    batch_id = (
        f"{tenant}_"
        f"{start_date_value.replace('-', '')}_"
        f"{end_date_value.replace('-', '')}"
    )

    run_id = str(uuid4())

    executed_at = datetime.now()

    # ---------------------------------------------------------
    # Check 1:
    # Business key must be unique in Silver
    # ---------------------------------------------------------

    duplicate_count = (
        df.groupBy(
            "_tenant_id",
            "fecha_proceso",
            "transporte",
            "ruta",
            "material",
            "tipo_entrega",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    # ---------------------------------------------------------
    # Check 2:
    # All normalized units must be ST
    # ---------------------------------------------------------

    invalid_unit_count = (
        df.filter(
            F.col("unidad_normalizada").isNull()
            | (
                F.upper(
                    F.col("unidad_normalizada")
                )
                != "ST"
            )
        )
        .count()
    )

    # ---------------------------------------------------------
    # Check 3:
    # Normalized quantity must be positive
    # ---------------------------------------------------------

    invalid_quantity_count = (
        df.filter(
            F.col(
                "cantidad_normalizada_st"
            ).isNull()
            | (
                F.col(
                    "cantidad_normalizada_st"
                )
                <= 0
            )
        )
        .count()
    )

    # ---------------------------------------------------------
    # Check 4:
    # Material enrichment should be complete
    # Warning because the pipeline already quarantines
    # unknown materials before Silver fact.
    # ---------------------------------------------------------

    missing_material_enrichment_count = (
        df.filter(
            F.col("descripcion").isNull()
            | F.col("categoria").isNull()
        )
        .count()
    )

    # ---------------------------------------------------------
    # Check 5:
    # Process date must be valid
    # ---------------------------------------------------------

    invalid_date_count = (
        df.filter(
            F.col(
                "fecha_proceso_date"
            ).isNull()
        )
        .count()
    )

    checks = [
        {
            "check_name":
                "business_key_uniqueness",
            "check_severity":
                "critical",
            "records_failed":
                duplicate_count,
        },
        {
            "check_name":
                "normalized_unit_is_st",
            "check_severity":
                "critical",
            "records_failed":
                invalid_unit_count,
        },
        {
            "check_name":
                "positive_normalized_quantity",
            "check_severity":
                "critical",
            "records_failed":
                invalid_quantity_count,
        },
        {
            "check_name":
                "material_enrichment_complete",
            "check_severity":
                "warning",
            "records_failed":
                missing_material_enrichment_count,
        },
        {
            "check_name":
                "valid_process_date",
            "check_severity":
                "critical",
            "records_failed":
                invalid_date_count,
        },
    ]

    quality_rows = []

    for check in checks:
        records_failed = (
            check["records_failed"]
        )

        quality_rows.append(
            Row(
                _run_id=run_id,
                _batch_id=batch_id,
                tenant_id=tenant,
                layer="silver",
                table_name="fact_deliveries",
                check_name=check[
                    "check_name"
                ],
                check_severity=check[
                    "check_severity"
                ],
                records_checked=records_checked,
                records_failed=records_failed,
                check_passed=(
                    records_failed == 0
                ),
                executed_at=executed_at,
            )
        )

    quality_df = (
        spark.createDataFrame(
            quality_rows,
            schema=QUALITY_LOG_SCHEMA,
        )
    )

    (
        quality_df.write
        .format("delta")
        .mode("append")
        .save(quality_logs_path)
    )

    # ---------------------------------------------------------
    # Execution summary
    # ---------------------------------------------------------

    print(
        f"Quality checks for tenant "
        f"{tenant.upper()}:"
    )

    (
        quality_df
        .select(
            "check_name",
            "check_severity",
            "records_checked",
            "records_failed",
            "check_passed",
        )
        .show(
            truncate=False
        )
    )

    critical_failures = [
        check
        for check in checks
        if (
            check["check_severity"]
            == "critical"
            and check[
                "records_failed"
            ]
            > 0
        )
    ]

    if (
        bool(
            config.quality
            .fail_on_critical
        )
        and critical_failures
    ):
        failed_names = [
            check["check_name"]
            for check
            in critical_failures
        ]

        raise RuntimeError(
            "Critical quality checks failed: "
            + ", ".join(failed_names)
        )

    print(
        f"Quality logs written to: "
        f"{quality_logs_path}"
    )

    return len(
        critical_failures
    ) == 0