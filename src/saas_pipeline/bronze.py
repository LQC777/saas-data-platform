from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("saas-data-platform")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_bronze(config):
    spark = create_spark_session()

    tenant = config.tenant.code.lower()

    start_date = config.execution.start_date or "all"
    end_date = config.execution.end_date or "all"

    batch_id = (
        f"{tenant}_"
        f"{start_date.replace('-', '')}_"
        f"{end_date.replace('-', '')}"
    )

    # ---------------------------------------------------------
    # 1. Bronze - entrega_productos
    # ---------------------------------------------------------

    input_path = (
        f"{config.paths.raw}/"
        "global_mobility_data_entrega_productos.csv"
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    df_bronze = (
        df
        .withColumn(
            "_tenant_id",
            F.lower(F.col("pais")),
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_file",
            F.input_file_name(),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
        .filter(
            F.col("_tenant_id") == tenant
        )
    )

    if config.execution.start_date:
        start_date = config.execution.start_date.replace("-", "")

        df_bronze = df_bronze.filter(
            F.col("fecha_proceso") >= start_date
        )

    if config.execution.end_date:
        end_date = config.execution.end_date.replace("-", "")

        df_bronze = df_bronze.filter(
            F.col("fecha_proceso") <= end_date
        )

    output_path = (
        f"{config.paths.bronze}/"
        f"{tenant}/"
        "entrega_productos"
    )

    # Idempotent write by processing date range
    replace_conditions = []

    if config.execution.start_date:
        replace_conditions.append(
            f"fecha_proceso >= '{start_date}'"
        )

    if config.execution.end_date:
        replace_conditions.append(
            f"fecha_proceso <= '{end_date}'"
        )

    writer = (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("fecha_proceso")
    )

    if replace_conditions:
        replace_where = " AND ".join(replace_conditions)

        writer.option(
            "replaceWhere",
            replace_where,
        ).save(output_path)
    else:
        writer.save(output_path)

    # ---------------------------------------------------------
    # 2. Bronze - materials_catalog
    # Shared reference data
    # ---------------------------------------------------------

    catalog_input_path = (
        f"{config.paths.raw}/"
        "materials_catalog.csv"
    )

    catalog_output_path = (
        f"{config.paths.bronze}/"
        "shared/"
        "materials_catalog"
    )

    df_catalog = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(catalog_input_path)
    )

    df_catalog_bronze = (
        df_catalog
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_file",
            F.input_file_name(),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
    )

    (
        df_catalog_bronze.write
        .format("delta")
        .mode("overwrite")
        .save(catalog_output_path)
    )

    # ---------------------------------------------------------
    # Execution summary
    # ---------------------------------------------------------

    print(
        f"Bronze preview for tenant "
        f"{tenant.upper()}:"
    )

    df_bronze.show(5, truncate=False)

    print(
        f"Total records for "
        f"{tenant.upper()}: {df_bronze.count()}"
    )

    print(
        f"Bronze Delta written to: "
        f"{output_path}"
    )

    print(
        f"Materials catalog written to: "
        f"{catalog_output_path}"
    )

    spark.stop()