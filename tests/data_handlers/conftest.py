import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.appName("test-dataframe-mapper").getOrCreate()


@pytest.fixture(
    scope="module",
    params=[
        (
            [
                {"PatientID": 1, "Date": "2021-01-01", "Value": 10},
                {"PatientID": 2, "Date": "2021-01-02", "Value": 20},
                {"PatientID": 3, "Date": None, "Value": 30},
                {"PatientID": 4, "Date": "2021-01-04", "Value": 40},
            ],
            ["PatientID"],
        ),
        (
            [
                {"reportnr": 19, "date": "2021-01-01", "sender": "John Doe"},
                {"reportnr": 19, "date": None, "sender": "Jane Green"},
                {"reportnr": 20, "date": "2021-01-03", "sender": "Jane Green"},
                {"reportnr": 21, "date": "2021-01-04", "sender": "John Doe"},
                {"reportnr": 22, "date": None, "sender": "Jane Doe"},
            ],
            ["reportnr", "sender"],
        ),
    ],
)
def input_data_and_primary_keys(spark_session, request):
    input_data, primary_keys = request.param
    return spark_session.createDataFrame(input_data), primary_keys
