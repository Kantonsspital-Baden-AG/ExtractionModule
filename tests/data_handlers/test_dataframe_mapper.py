import pytest
from pyspark.sql.functions import col
from functools import reduce

from extraction.data_handlers.dataframe_mapper import DataFrameMapper


def test_fit_function_sets_primary_keys(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper fit method sets the primary keys.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    mapper.fit(input_data)
    assert mapper.primary_keys == primary_keys


def test_fit_function_sets_mapped_primary_key(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper fit method sets the mapped primary key.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    mapper.fit(input_data)

    assert isinstance(
        mapper.mapped_primary_key, str
    ), "Mapped primary key is not a string."
    assert (
        mapper.mapped_primary_key not in input_data.columns
    ), "Mapped primary key is in the input data columns."


def test_fit_function_sets_concordance_table_structure(
    input_data_and_primary_keys,
) -> None:
    """Test DataFrameMapper fit method sets the concordance table.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    mapper.fit(input_data)
    assert isinstance(
        mapper.concordance_table, dict
    ), "Concordance table is not a dictionary."
    assert (
        len(mapper.concordance_table) == input_data.count()
    ), "Concordance table length is not equal to the input data count."


def test_fit_function_sets_concordance_table_values(
    input_data_and_primary_keys,
) -> None:
    """Test DataFrameMapper fit method sets the concordance table values.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    input_data = input_data.orderBy(primary_keys)

    mapper = DataFrameMapper(primary_keys=primary_keys)
    mapper.fit(input_data)

    expected_mapper = DataFrameMapper(primary_keys=primary_keys)
    assert mapper.concordance_table == {
        expected_mapper.id_gen.generate_next(): {
            p_key: row[p_key] for p_key in primary_keys
        }
        for row in input_data.collect()
    }


def test_generate_concordance_table_structure(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper generate_concordance_table method sets the concordance table.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    concordance_table = mapper._generate_concordance_table(input_data)

    assert isinstance(concordance_table, dict), "Concordance table is not a dictionary."

    # The concordance table has the form {<excel_index>: {<primary_key>: <value>, ...}, ...}
    for key, values in concordance_table.items():
        assert isinstance(key, str), "Concordance table key is not a string."
        assert isinstance(values, dict), "Concordance table value is not a dictionary."
        assert len(values) == len(
            primary_keys
        ), "Concordance table value length is not equal to the primary keys length."
        assert all(
            [k in values for k in primary_keys]
        ), "Concordance table value does not contain all primary keys."


def test_generate_concordance_table_values(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper fit method sets the concordance table values.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    input_data = input_data.orderBy(primary_keys)

    mapper = DataFrameMapper(primary_keys=primary_keys)
    concordance_table = mapper._generate_concordance_table(input_data)

    expected_mapper = DataFrameMapper(primary_keys=primary_keys)
    assert concordance_table == {
        expected_mapper.id_gen.generate_next(): {
            p_key: row[p_key] for p_key in primary_keys
        }
        for row in input_data.collect()
    }


def test_fit_transform_is_fit_and_transform(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper fit_transform method.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper_fit_transform = DataFrameMapper(primary_keys=primary_keys)
    fit_transformed_data = mapper_fit_transform.fit_transform(input_data).cache()

    mapper_fit_and_transform = DataFrameMapper(primary_keys=primary_keys)
    mapper_fit_and_transform.fit(input_data)
    fit_and_transformed_data = mapper_fit_and_transform.transform(input_data).cache()

    assert sorted(
        fit_transformed_data.collect(),
        key=lambda x: x[mapper_fit_transform.mapped_primary_key],
    ) == sorted(
        fit_and_transformed_data.collect(),
        key=lambda x: x[mapper_fit_and_transform.mapped_primary_key],
    )


def test_transform_without_fit_raises_exception(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper transform method without fit.
    It is expected to raise an exception.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)

    with pytest.raises(Exception):
        mapper.transform(input_data)


def test_transform_with_fit_structure(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper transform method with fit sets the transformed DataFrame structure.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    mapper.fit(input_data)
    transformed_data = mapper.transform(input_data)

    assert set(transformed_data.columns) == set(
        [c for c in input_data.columns if c not in primary_keys]
        + [mapper.mapped_primary_key]
    ), "Transformed DataFrame columns are incorrect."
    assert (
        transformed_data.count() == input_data.count()
    ), "Transformed DataFrame count is greater than the input data count."
    assert (
        set(
            r[mapper.mapped_primary_key]
            for r in transformed_data.select(mapper.mapped_primary_key).collect()
        )
        == set(mapper.concordance_table.keys())
    ), "Transformed DataFrame primary key values are not equal to the concordance table keys."


def test_transform_with_fit_values(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper transform method with fit sets the transformed DataFrame values.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    mapper.fit(input_data)
    transformed_data = mapper.transform(input_data)

    for row in transformed_data.collect():
        concordance_values = mapper.concordance_table[row[mapper.mapped_primary_key]]
        input_values = input_data.filter(
            reduce(
                lambda x, y: x & y,
                [col(k) == v for k, v in concordance_values.items()],
            )
        )
        assert input_values.count() == 1, "Input values count is not equal to 1."
        assert all(
            [
                row[c] == input_values.select(c).collect()[0][c]
                for c in input_values.columns
                if c not in primary_keys
            ]
        ), "Transformed DataFrame values are not equal to the input values."


def test_transform_inverse_without_fit_raises_exception(
    input_data_and_primary_keys,
) -> None:
    """Test DataFrameMapper transform_inverse method without fit.
    It is expected to raise an exception.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)

    with pytest.raises(Exception):
        mapper.transform_inverse(input_data)


def test_transform_inverse_with_fit_structure(input_data_and_primary_keys) -> None:
    """Test DataFrameMapper transform_inverse method with fit sets the transformed DataFrame structure.

    Args:
        input_data (DataFrame): The input data.
        primary_keys (list, optional): The primary keys. Defaults to ["PatientID"].
    """
    input_data, primary_keys = input_data_and_primary_keys
    mapper = DataFrameMapper(primary_keys=primary_keys)
    transformed_data = mapper.fit_transform(input_data)

    inverse_transformed_data = mapper.transform_inverse(transformed_data)

    assert set(inverse_transformed_data.columns) == set(
        input_data.columns
    ), "Inverse transformed DataFrame columns are incorrect."
    assert (
        inverse_transformed_data.count() == input_data.count()
    ), "Inverse transformed DataFrame count is greater than the input data count."

    for column in [c for c in input_data.columns if c not in primary_keys]:
        assert (
            inverse_transformed_data.join(
                input_data.withColumnRenamed(column, f"{column}_input"),
                on=primary_keys,
                how="inner",
            )
            .filter(col(column) != col(f"{column}_input"))
            .count()
            == 0
        ), f"Inverse transformed DataFrame column '{column}' values are not equal to the input data column values."
