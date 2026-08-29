from delta.tables import DeltaTable
from pyspark.sql import functions as F

VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]


def run_silver(spark, config):
    tenant = config.tenant.code.lower()

    # ---------------------------------------------------------
    # Paths
    # ---------------------------------------------------------

    bronze_path = (
        f"{config.paths.bronze}/"
        f"{tenant}/"
        "entrega_productos"
    )

    catalog_bronze_path = (
        f"{config.paths.bronze}/"
        "shared/"
        "materials_catalog"
    )

    fact_silver_path = (
        f"{config.paths.silver}/"
        f"{tenant}/"
        "fact_deliveries"
    )

    dim_silver_path = (
        f"{config.paths.silver}/"
        f"{tenant}/"
        "dim_materials"
    )

    quarantine_path = (
        f"{config.paths.quarantine_root}/"
        "silver_quarantine/"
        f"{tenant}/"
        "fact_deliveries"
    )

    # ---------------------------------------------------------
    # Read Bronze
    # ---------------------------------------------------------

    df_bronze = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    df_catalog = (
        spark.read
        .format("delta")
        .load(catalog_bronze_path)
    )

    # ---------------------------------------------------------
    # 1. Remove exact duplicates
    # ---------------------------------------------------------

    original_columns = [
        "pais",
        "fecha_proceso",
        "transporte",
        "ruta",
        "tipo_entrega",
        "material",
        "precio",
        "cantidad",
        "unidad",
    ]

    df_deduplicated = (
        df_bronze
        .dropDuplicates(original_columns)
    )

    # ---------------------------------------------------------
    # 2. Parse and normalize fields
    # ---------------------------------------------------------

    df_transformed = (
        df_deduplicated
        .withColumn(
            "fecha_proceso_date",
            F.to_date(
                F.col("fecha_proceso").cast("string"),
                "yyyyMMdd",
            ),
        )
        .withColumn(
            "precio",
            F.col("precio").cast("double"),
        )
        .withColumn(
            "cantidad",
            F.col("cantidad").cast("double"),
        )
    )

    # ---------------------------------------------------------
    # 3. Quarantine basic invalid records
    # ---------------------------------------------------------

    basic_invalid_condition = (
        F.col("fecha_proceso_date").isNull()
        | F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
        | F.col("precio").isNull()
    )

    df_basic_quarantine = (
        df_transformed
        .filter(basic_invalid_condition)
        .withColumn(
            "_quarantine_reason",
            F.when(
                F.col("fecha_proceso_date").isNull(),
                F.lit("INVALID_PROCESS_DATE"),
            )
            .when(
                F.col("cantidad").isNull()
                | (F.col("cantidad") <= 0),
                F.lit("INVALID_QUANTITY"),
            )
            .when(
                F.col("precio").isNull(),
                F.lit("NULL_PRICE"),
            )
            .otherwise(
                F.lit("UNKNOWN"),
            ),
        )
    )

    df_valid = (
        df_transformed
        .filter(~basic_invalid_condition)
    )

    # ---------------------------------------------------------
    # 4. Discard delivery types outside business scope
    # ---------------------------------------------------------

    df_valid_delivery = (
        df_valid
        .filter(
            F.col("tipo_entrega").isin(
                VALID_DELIVERY_TYPES
            )
        )
    )

    discarded_count = (
        df_valid
        .filter(
            ~F.col("tipo_entrega").isin(
                VALID_DELIVERY_TYPES
            )
        )
        .count()
    )

    # ---------------------------------------------------------
    # 5. Normalize units to ST
    #
    # 1 CS = 20 ST
    # ---------------------------------------------------------

    df_normalized = (
        df_valid_delivery
        .withColumn(
            "cantidad_normalizada_st",
            F.when(
                F.upper(F.col("unidad")) == "CS",
                F.col("cantidad") * F.lit(20.0),
            ).otherwise(
                F.col("cantidad")
            ),
        )
        .withColumn(
            "unidad_normalizada",
            F.lit("ST"),
        )
        .withColumn(
            "is_routine_delivery",
            F.col("tipo_entrega").isin(
                ["ZPRE", "ZVE1"]
            ),
        )
        .withColumn(
            "is_bonus_delivery",
            F.col("tipo_entrega").isin(
                ["Z04", "Z05"]
            ),
        )
    )

    # ---------------------------------------------------------
    # 6. Silver dim_materials - SCD Type 2
    # ---------------------------------------------------------

    df_dim_materials = (
        df_catalog
        .select(
            "material",
            "descripcion",
            "categoria",
            F.col("precio_base").cast("double"),
            F.col("valid_from").cast("date"),
            F.col("valid_to").cast("date"),
            F.col("is_current").cast("boolean"),
        )
        .dropDuplicates(
            ["material", "valid_from"]
        )
    )

    if DeltaTable.isDeltaTable(
        spark,
        dim_silver_path,
    ):
        dim_delta = DeltaTable.forPath(
            spark,
            dim_silver_path,
        )

        (
            dim_delta.alias("target")
            .merge(
                df_dim_materials.alias("source"),
                """
                target.material = source.material
                AND target.valid_from = source.valid_from
                """,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        (
            df_dim_materials.write
            .format("delta")
            .mode("overwrite")
            .save(dim_silver_path)
        )

    # ---------------------------------------------------------
    # 7. Temporal join with SCD Type 2 dimension
    # ---------------------------------------------------------

    df_dim = (
        spark.read
        .format("delta")
        .load(dim_silver_path)
        .select(
            F.col("material").alias(
                "dim_material"
            ),
            "descripcion",
            "categoria",
            "precio_base",
            "valid_from",
            "valid_to",
            "is_current",
        )
    )

    temporal_condition = (
        (
            F.col("fact.material")
            == F.col("dim.dim_material")
        )
        & (
            F.col("fact.fecha_proceso_date")
            >= F.col("dim.valid_from")
        )
        & (
            F.col("fact.fecha_proceso_date")
            <= F.col("dim.valid_to")
        )
    )

    df_enriched = (
        df_normalized.alias("fact")
        .join(
            df_dim.alias("dim"),
            temporal_condition,
            "left",
        )
    )

    # ---------------------------------------------------------
    # 8. Materials not found in catalog -> quarantine
    # ---------------------------------------------------------

    df_material_quarantine = (
        df_enriched
        .filter(
            F.col("dim_material").isNull()
        )
        .drop("dim_material")
        .withColumn(
            "_quarantine_reason",
            F.lit("MATERIAL_NOT_IN_CATALOG"),
        )
    )

    df_fact = (
        df_enriched
        .filter(
            F.col("dim_material").isNotNull()
        )
        .drop("dim_material")
    )

    # Align both quarantine DataFrames before union
    quarantine_columns = (
        df_basic_quarantine.columns
    )

    df_material_quarantine_aligned = (
        df_material_quarantine
        .select(
            *[
                F.col(column)
                if column
                in df_material_quarantine.columns
                else F.lit(None).alias(column)
                for column in quarantine_columns
            ]
        )
    )

    df_quarantine = (
        df_basic_quarantine
        .unionByName(
            df_material_quarantine_aligned,
            allowMissingColumns=True,
        )
    )

    # ---------------------------------------------------------
    # 9. MERGE INTO Silver fact_deliveries
    #
    # Business key:
    # tenant_id + fecha_proceso + transporte +
    # ruta + material + tipo_entrega
    # ---------------------------------------------------------

    merge_condition = """
        target._tenant_id = source._tenant_id
        AND target.fecha_proceso = source.fecha_proceso
        AND target.transporte = source.transporte
        AND target.ruta = source.ruta
        AND target.material = source.material
        AND target.tipo_entrega = source.tipo_entrega
    """

    if DeltaTable.isDeltaTable(
        spark,
        fact_silver_path,
    ):
        fact_delta = DeltaTable.forPath(
            spark,
            fact_silver_path,
        )

        (
            fact_delta.alias("target")
            .merge(
                df_fact.alias("source"),
                merge_condition,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        (
            df_fact.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("fecha_proceso")
            .save(fact_silver_path)
        )

    # ---------------------------------------------------------
    # 10. Write quarantine
    # ---------------------------------------------------------

    (
        df_quarantine.write
        .format("delta")
        .mode("overwrite")
        .save(quarantine_path)
    )

    # ---------------------------------------------------------
    # Execution summary
    # ---------------------------------------------------------

    fact_count = (
        spark.read
        .format("delta")
        .load(fact_silver_path)
        .count()
    )

    quarantine_count = df_quarantine.count()

    print(
        f"Silver fact_deliveries records for "
        f"{tenant.upper()}: {fact_count}"
    )

    print(
        f"Silver quarantine records for "
        f"{tenant.upper()}: {quarantine_count}"
    )

    print(
        f"Discarded delivery type records for "
        f"{tenant.upper()}: {discarded_count}"
    )

    print(
        f"Silver fact_deliveries written to: "
        f"{fact_silver_path}"
    )

    print(
        f"Silver dim_materials written to: "
        f"{dim_silver_path}"
    )

    print(
        f"Silver quarantine written to: "
        f"{quarantine_path}"
    )