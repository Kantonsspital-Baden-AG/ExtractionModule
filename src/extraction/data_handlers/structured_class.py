from pyspark.sql import DataFrame
from pyspark.sql.functions import col, get_json_object, to_json, struct, when
from functools import reduce

from extraction.utils.loggings import logger
from extraction.utils.helper import generate_unique_name


class StructuredDataHandler:
    """Base class for structured data classes"""

    primary_keys: list[str]
    input_columns: list[str]

    def __init__(
        self, primary_keys: list[str], input_columns: list[str] | None = None
    ) -> None:
        """Initialize the StructuredDataHandler.

        Args:
            primary_keys (list[str]): The primary keys to use for the class.
            input_columns (list[str]): The input columns to use for the class.
        """
        self.primary_keys = self._validate_primary_keys(primary_keys)
        self.input_columns = self._validate_input_columns(input_columns)

    def _validate_primary_keys(self, primary_keys: list[str]) -> None:
        """Validate the primary keys.

        Args:
            primary_keys (list[str]): The primary keys to validate.
        """
        if not primary_keys:
            raise ValueError("No primary keys provided.")

        # Primary keys must be a list
        if not isinstance(primary_keys, list):
            raise ValueError(
                f"Primary keys must be a list, not a {type(primary_keys)}."
            )

        # Each entry in the primary keys list must be a string
        for key in primary_keys:
            if not isinstance(key, str):
                raise ValueError(f"Primary key must be a string, not a {type(key)}.")

        return primary_keys

    def _validate_input_columns(self, input_columns: list[str] | None = None) -> None:
        """Validate the input columns.

        Args:
            input_columns (list[str]): The input columns to validate.
        """
        if input_columns is None:
            raise ValueError("No input columns provided.")

        # Input columns must be a list
        if not isinstance(input_columns, list):
            raise ValueError(
                f"Input columns must be a list, not a {type(input_columns)}."
            )

        return input_columns

    @staticmethod
    def _unpack_json(df: DataFrame, json_column: str, keys_lookup: dict) -> DataFrame:
        """Unpack a JSON(str) column into multiple columns.

        Args:
            df (DataFrame): The input DataFrame.
            json_column (str): The JSON column to unpack.
            keys_lookup (dict): The keys to unpack: {"json_key": "new_column_name"}.

        Returns:
            DataFrame: The unpacked DataFrame.
        """
        try:
            for k, v in keys_lookup.items():
                df = df.withColumn(
                    v,
                    get_json_object(col(json_column), "$." + k),
                )
            return df
        except Exception as e:
            msg = f"Error unpacking JSON: {e}"
            logger.error(msg)
            raise Exception(msg)

    @staticmethod
    def _pack_json(df: DataFrame, json_column: str, keys_lookup: dict) -> DataFrame:
        """Pack multiple columns into a JSON(str) column.

        If a column contains a NULL value, the corresponding key will not be included in the JSON.
        If all columns contain NULL values, the JSON column will be NULL.

        Args:
            df (DataFrame): The input DataFrame.
            json_column (str): The JSON column to pack.
            keys_lookup (dict): The keys to pack, {"column_name": "json_key"}.

        Returns:
            DataFrame: The packed DataFrame.
        """
        try:
            json_keys = [col(k).cast("string").alias(v) for k, v in keys_lookup.items()]

            return df.withColumn(
                json_column,
                when(
                    reduce(lambda x, y: x | y, [c.isNotNull() for c in json_keys]),
                    to_json(struct(*json_keys)),
                ).otherwise(None),
            )
        except Exception as e:
            msg = f"Error packing JSON: {e}"
            logger.error(msg)
            raise Exception(msg)

    def _prepare_input_json(
        self,
        df: DataFrame,
        primary_keys: list[str] | None = None,
        input_columns: list[str] | None = None,
    ) -> dict:
        """Prepare the input JSON for the language model.

        Args:
            df (DataFrame): The input DataFrame.
            primary_keys (list[str]|None): The primary keys to use for the input JSON.
            input_columns (list[str]|None): The input columns to use for the input JSON.

        Returns:
            dict: The input JSON.
        """
        if not primary_keys:
            primary_keys = self.primary_keys

        if not input_columns:
            input_columns = self.input_columns

        try:
            existent_columns = [*df.columns, *primary_keys, *input_columns]

            key_column_name = generate_unique_name(
                seed="key", existing_names=existent_columns
            )
            value_column_name = generate_unique_name(
                seed="value", existing_names=existent_columns
            )

            return {
                f"{row.key}" if len(primary_keys) == 1 else f"({row.key})": row.value
                if len(input_columns) == 1
                else f"({row.value})"
                for row in df.selectExpr(
                    f"concat_ws(',', {','.join(primary_keys)}) as {key_column_name}",
                    f"concat_ws(',', {','.join(input_columns)}) as {value_column_name}",
                ).collect()
            }
        except Exception as e:
            msg = f"Error preparing input JSON: {e}"
            logger.error(msg)
            raise Exception(msg)
