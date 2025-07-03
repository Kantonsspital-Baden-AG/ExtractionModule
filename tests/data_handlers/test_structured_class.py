import pytest
from pyspark.sql.functions import col, to_json, struct

from extraction.data_handlers.structured_class import StructuredDataHandler


def test_structured_data_handler_invalid_input():
    with pytest.raises(ValueError):
        StructuredDataHandler(None)

    with pytest.raises(ValueError):
        StructuredDataHandler([])

    with pytest.raises(ValueError):
        StructuredDataHandler(["PatientID"], None)

    with pytest.raises(ValueError):
        StructuredDataHandler(["PatientID"], "")


def test_structured_data_handler_valid_input(input_data_and_primary_keys) -> None:
    """Test the StructuredDataHandler class with valid input."""
    input_data, primary_keys = input_data_and_primary_keys

    handler = StructuredDataHandler(primary_keys=primary_keys, input_columns=[])
    assert (
        handler.primary_keys == primary_keys
    ), f"Primary keys: {handler.primary_keys}, Expected: {primary_keys}"
    assert (
        handler.input_columns == []
    ), f"Input columns: {handler.input_columns}, Expected: []"

    non_primary_keys = [c for c in input_data.columns if c not in primary_keys]
    handler = StructuredDataHandler(
        primary_keys=primary_keys, input_columns=non_primary_keys
    )
    assert (
        handler.primary_keys == primary_keys
    ), f"Primary keys: {handler.primary_keys}, Expected: {primary_keys}"
    assert (
        handler.input_columns == non_primary_keys
    ), f"Input columns: {handler.input_columns}, Expected: {non_primary_keys}"


def test_unpacking_json(spark_session, input_data_and_primary_keys) -> None:
    """Test the unpacking of a JSON object."""

    input_data, _ = input_data_and_primary_keys

    # Create a new dataframe with a single json column
    json_column = "json_column"
    keys_lookup = {c: f"{c}_new" for c in input_data.columns}

    json_keys = [col(k).cast("string").alias(v) for k, v in keys_lookup.items()]
    json_data = input_data.withColumn(json_column, to_json(struct(*json_keys))).select(
        json_column
    )

    # Unpack the json column
    unpacked_data = StructuredDataHandler._unpack_json(
        json_data, json_column, keys_lookup
    )

    # Check that the unpacked data has the correct columns
    expected_columns = [json_column] + list(keys_lookup.values())
    assert set(unpacked_data.columns) == set(
        expected_columns
    ), f"Columns: {unpacked_data.columns}, Expected: {expected_columns}"


def test_packing_json(spark_session, input_data_and_primary_keys) -> None:
    """Test the packing of a JSON object."""

    input_data, primary_keys = input_data_and_primary_keys

    # Create a new dataframe with a single json column
    json_column = "json_column"
    keys_lookup = {c: f"{c}_new" for c in input_data.columns}

    json_keys = [col(k).cast("string").alias(v) for k, v in keys_lookup.items()]
    json_data = input_data.withColumn(json_column, to_json(struct(*json_keys))).select(
        json_column, *primary_keys
    )

    # Pack the input data
    packed_data = StructuredDataHandler._pack_json(
        input_data, json_column=json_column, keys_lookup=keys_lookup
    ).select(json_column, *primary_keys)

    # Check that the packed data has the correct columns
    expected_columns = [json_column]
    assert (
        packed_data.join(
            json_data.withColumnRenamed(json_column, "json_column_expected"),
            on=primary_keys,
        )
        .filter(col(json_column) != col("json_column_expected"))
        .count()
        == 0
    ), f"Columns: {packed_data.columns}, Expected: {expected_columns}"
