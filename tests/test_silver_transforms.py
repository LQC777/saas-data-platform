import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("saas-data-platform-tests")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_cs_to_st_conversion(spark):
    data = [
        ("CS", 2.0),
        ("ST", 15.0),
    ]

    df = spark.createDataFrame(
        data,
        ["unidad", "cantidad"],
    )

    result = (
        df.withColumn(
            "cantidad_normalizada_st",
            F.when(
                F.upper(F.col("unidad")) == "CS",
                F.col("cantidad") * 20.0,
            ).otherwise(F.col("cantidad")),
        )
        .select(
            "unidad",
            "cantidad_normalizada_st",
        )
        .collect()
    )

    assert result[0]["cantidad_normalizada_st"] == 40.0
    assert result[1]["cantidad_normalizada_st"] == 15.0


def test_valid_delivery_type_filter(spark):
    data = [
        ("ZPRE",),
        ("ZVE1",),
        ("Z04",),
        ("Z05",),
        ("COBR",),
        ("Z99",),
    ]

    df = spark.createDataFrame(
        data,
        ["tipo_entrega"],
    )

    valid_types = [
        "ZPRE",
        "ZVE1",
        "Z04",
        "Z05",
    ]

    result = (
        df.filter(
            F.col("tipo_entrega").isin(
                valid_types
            )
        )
        .select("tipo_entrega")
        .collect()
    )

    values = {
        row["tipo_entrega"]
        for row in result
    }

    assert values == {
        "ZPRE",
        "ZVE1",
        "Z04",
        "Z05",
    }


def test_invalid_quantity_detection(spark):
    data = [
        (10.0,),
        (0.0,),
        (-5.0,),
        (None,),
    ]

    df = spark.createDataFrame(
        data,
        ["cantidad"],
    )

    invalid = df.filter(
        F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
    )

    assert invalid.count() == 3


def test_delivery_flags(spark):
    data = [
        ("ZPRE",),
        ("ZVE1",),
        ("Z04",),
        ("Z05",),
    ]

    df = spark.createDataFrame(
        data,
        ["tipo_entrega"],
    )

    result = (
        df.withColumn(
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
        .collect()
    )

    assert result[0]["is_routine_delivery"] is True
    assert result[0]["is_bonus_delivery"] is False

    assert result[2]["is_routine_delivery"] is False
    assert result[2]["is_bonus_delivery"] is True