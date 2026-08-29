from delta.tables import DeltaTable
from pyspark.sql import functions as F


def run_gold(spark, config):
    tenant = config.tenant.code.lower()

    # ---------------------------------------------------------
    # Paths
    # ---------------------------------------------------------

    silver_path = (
        f"{config.paths.silver}/"
        f"{tenant}/"
        "fact_deliveries"
    )

    gold_path = (
        f"{config.paths.gold}/"
        f"{tenant}/"
        "daily_metrics_by_delivery_type"
    )

    # ---------------------------------------------------------
    # 1. Read Silver fact_deliveries
    # ---------------------------------------------------------

    df_silver = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # ---------------------------------------------------------
    # 2. Apply execution date range
    #
    # Silver preserves fecha_proceso as YYYYMMDD.
    # ---------------------------------------------------------

    if config.execution.start_date:
        start_date = (
            config.execution.start_date
            .replace("-", "")
        )

        df_silver = df_silver.filter(
            F.col("fecha_proceso") >= start_date
        )

    if config.execution.end_date:
        end_date = (
            config.execution.end_date
            .replace("-", "")
        )

        df_silver = df_silver.filter(
            F.col("fecha_proceso") <= end_date
        )

    # ---------------------------------------------------------
    # 3. Build Gold metrics
    #
    # Granularity:
    # tenant_id + fecha_proceso + tipo_entrega
    #
    # total_units:
    #   normalized quantity already expressed in ST
    #
    # total_revenue:
    #   normalized quantity * transaction price
    #
    # IMPORTANT:
    # precio_base from dim_materials is NOT used
    # for revenue.
    # ---------------------------------------------------------

    df_gold = (
        df_silver
        .groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.sum(
                F.col("cantidad_normalizada_st")
            ).alias(
                "total_units"
            ),
            F.sum(
                F.col("cantidad_normalizada_st")
                * F.col("precio")
            ).alias(
                "total_revenue"
            ),
            F.countDistinct(
                "ruta"
            ).alias(
                "active_routes"
            ),
            F.countDistinct(
                "transporte"
            ).alias(
                "active_transports"
            ),
        )
        .orderBy(
            "fecha_proceso",
            "tipo_entrega",
        )
    )

    # ---------------------------------------------------------
    # 4. Idempotent Gold write
    #
    # Gold is derived data, therefore the architecture
    # requires complete recomputation of the requested
    # date partitions.
    # ---------------------------------------------------------

    replace_conditions = []

    if config.execution.start_date:
        start_date = (
            config.execution.start_date
            .replace("-", "")
        )

        replace_conditions.append(
            f"fecha_proceso >= '{start_date}'"
        )

    if config.execution.end_date:
        end_date = (
            config.execution.end_date
            .replace("-", "")
        )

        replace_conditions.append(
            f"fecha_proceso <= '{end_date}'"
        )

    gold_exists = DeltaTable.isDeltaTable(
        spark,
        gold_path,
    )

    if gold_exists and replace_conditions:
        replace_where = " AND ".join(
            replace_conditions
        )

        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                replace_where,
            )
            .save(gold_path)
        )

    elif gold_exists:
        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

    else:
        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("fecha_proceso")
            .save(gold_path)
        )

    # ---------------------------------------------------------
    # 5. Execution summary
    # ---------------------------------------------------------

    df_gold_result = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    print(
        f"Gold preview for tenant "
        f"{tenant.upper()}:"
    )

    (
        df_gold_result
        .orderBy(
            "fecha_proceso",
            "tipo_entrega",
        )
        .show(
            20,
            truncate=False,
        )
    )

    print(
        f"Gold records for "
        f"{tenant.upper()}: "
        f"{df_gold_result.count()}"
    )

    print(
        f"Gold daily_metrics_by_delivery_type "
        f"written to: {gold_path}"
    )