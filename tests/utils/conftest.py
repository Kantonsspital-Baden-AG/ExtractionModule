import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session."""
    return SparkSession.builder.master("local[2]").appName("test-utils").getOrCreate()


@pytest.fixture(scope="module")
def input_data(spark_session):
    """Create a DataFrame with some input data.

    Args:
        spark_session (SparkSession): The Spark session.

    Returns:
        DataFrame: A DataFrame with some input data.
    """
    data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David"),
        (5, "Eve"),
        (6, "Frank"),
        (7, "Grace"),
        (8, "Helen"),
        (9, "Ivy"),
        (10, "Jack"),
        (11, "Kate"),
    ]
    return spark_session.createDataFrame(data, schema=["id", "name"])
