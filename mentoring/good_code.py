from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]


def transform_deliveries(df: DataFrame, country: str) -> DataFrame:
    """Transform delivery data using native Spark operations."""

    return (
        df.filter(F.lower(F.col("pais")) == country.lower())
        .filter(F.col("tipo_entrega").isin(VALID_DELIVERY_TYPES))
        .filter(F.col("cantidad").isNotNull())
        .filter(F.col("cantidad") > 0)
        .filter(F.col("precio").isNotNull())
        .withColumn(
            "cantidad_st",
            F.when(
                F.upper(F.col("unidad")) == "CS",
                F.col("cantidad") * F.lit(20.0),
            ).otherwise(F.col("cantidad")),
        )
        .withColumn(
            "total",
            F.col("cantidad_st") * F.col("precio"),
        )
        .select(
            "pais",
            F.col("fecha_proceso").alias("fecha"),
            "material",
            "cantidad_st",
            "total",
        )
    )


def process(
    spark: SparkSession,
    file_path: str,
    country: str,
    output_path: str,
) -> DataFrame:
    """Read, transform and persist delivery data."""

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    result = transform_deliveries(
        df=df,
        country=country,
    )

    (
        result.write
        .format("delta")
        .mode("overwrite")
        .save(f"{output_path}/{country.lower()}")
    )

    return result