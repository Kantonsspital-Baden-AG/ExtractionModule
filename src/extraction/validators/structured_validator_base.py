from abc import ABC, abstractmethod
from typing import Callable
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, lit, concat, when
from pyspark.sql.types import BooleanType, StringType, StructType, StructField

from extraction.data_handlers.structured_class import StructuredDataHandler
from extraction.utils.helper import generate_unique_name

list_rules_type = list[Callable[[str], bool]]


class ExtractionValidatorBase(ABC, StructuredDataHandler):
    SUCCESS = "<SUCCESS>"

    primary_keys: list[str] = []
    input_columns: list[str] | None = None

    column_rules: list_rules_type = []

    def __init__(
        self,
        primary_keys: list[str],
        input_columns: list[str] | None = None,
        column_rules: dict[str, list_rules_type] = {},
    ) -> None:
        """Initialize the StructuredDataHandler.

        Args:
            primary_keys (list[str]): The primary keys to use for the class.
            input_columns (list[str]): The input columns to use for the class.
        """
        super().__init__(primary_keys=primary_keys, input_columns=input_columns)

        self.column_rules = self._validate_column_rules(column_rules)

    def _validate_column_rules(
        self, rules: dict[str, list_rules_type]
    ) -> dict[str, list_rules_type]:
        """Validate the rules.

        Args:
            rules (dict[str, list[Callable[[str], bool]]): The rules to validate.
        """

        if isinstance(rules, dict):
            for column, column_rules in rules.items():
                if not (isinstance(column, str) and (column in self.input_columns)):
                    raise ValueError(
                        f"Column must be a string, and in the input columns: {self.input_columns}."
                    )

                if not isinstance(column_rules, list):
                    raise ValueError(
                        f"Rules must be a list, not a {type(column_rules)}."
                    )

                for rule in column_rules:
                    if not callable(rule):
                        raise ValueError(
                            f"Rule must be a callable function of the form: Callable[[str], bool], not a {type(rule)}."
                        )

            return rules

        raise ValueError(
            f"""Rules must be a dict of the form {{"<input_column>" : ["<rules>"]}}, not a {type(rules)}."""
        )

    def _run_rules_on_col_with_response(
        self,
        value: str,
        rules: list_rules_type,
    ) -> tuple[bool, str]:
        """Run the rules on the column and return the error message.

        Args:
            value (str): The value to validate.
            rules (list[Callable[[str], bool]]): The rules to apply to the value.

        Returns:
            tuple[bool,str]: A tuple containing the validation result and the error message.
        """
        for rule in rules:
            is_valid, error_msg = rule(value)
            if not is_valid:
                return is_valid, error_msg

        return True, self.SUCCESS

    def validate_with_response(
        self,
        df: DataFrame,
        primary_keys: list[str] | None = None,
        input_columns: list[str] | None = None,
        column_rules: dict[str, list[Callable]] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
        """Validate the entires of the dataframe, based on the rules,
        and return the invalid rows with the error message.

        Args:
            df (DataFrame): The dataframe to validate.
            primary_keys (list[str], optional): The primary keys to validate. Defaults to None.
            input_columns (list[str], optional): The columns to validate. Defaults to None.
            column_rules (dict[str, list[Callable[[str], bool]]], optional): The rules to apply to the columns. Defaults to None.

        Returns:
            DataFrame: The validated dataframe with the error message.
        """
        if not primary_keys:
            primary_keys = self.primary_keys

        if not column_rules:
            column_rules = self.column_rules

        if not input_columns:
            input_columns = self.input_columns

        entity_types_validation_columns = {
            column: {
                "validation_column": generate_unique_name(
                    seed=f"{column}_validation_",
                    existing_names=[
                        *[column for column in df.columns],
                        *primary_keys,
                        *input_columns,
                    ],
                ),
                "error_column": generate_unique_name(
                    seed=f"{column}_error_",
                    existing_names=[
                        *[column for column in df.columns],
                        *primary_keys,
                        *input_columns,
                    ],
                ),
            }
            for column in input_columns
        }

        for column, validation_dict in entity_types_validation_columns.items():
            validate_column_udf = udf(
                lambda value: {
                    k: v
                    for k, v in zip(
                        ["valid", "error"],
                        self._run_rules_on_col_with_response(
                            value, column_rules.get(column, [])
                        ),
                    )
                },
                # The return type is a tuple of bool and str
                StructType(
                    [
                        StructField("valid", BooleanType()),
                        StructField("error", StringType()),
                    ]
                ),
            )

            df = (
                df.withColumn(
                    validation_dict["validation_column"],
                    validate_column_udf(col(column)),
                )
                .withColumn(
                    validation_dict["error_column"],
                    col(validation_dict["validation_column"]).getField("error"),
                )
                .withColumn(
                    validation_dict["validation_column"],
                    col(validation_dict["validation_column"]).getField("valid"),
                )
            )

        validation_columns = [
            validation_dict["validation_column"]
            for validation_dict in entity_types_validation_columns.values()
        ]
        error_columns = [
            validation_dict["error_column"]
            for validation_dict in entity_types_validation_columns.values()
        ]

        # Separate valid and invalid rows: filter rows that have a True in all the validation columns
        valid_rows = df.filter(
            reduce(
                lambda x, y: x & y,
                (
                    col(column)
                    for column in [
                        c["validation_column"]
                        for c in entity_types_validation_columns.values()
                    ]
                ),
            )
        ).drop(*[*validation_columns, *error_columns])

        invalid_rows = df.join(valid_rows, on=primary_keys, how="left_anti")

        if invalid_rows.count() > 0:
            # Update the entries in the input_columns by pre-pending the error message:
            # Collect the first error message from the first colum that has a False value in the validation column
            for column, validation_dict in entity_types_validation_columns.items():
                # Extract the invalid rows that contain the error in the column <column>
                invalid_rows = invalid_rows.withColumn(
                    column,
                    when(
                        col(validation_dict["validation_column"]) == False,  # noqa: E712
                        concat(
                            col(validation_dict["error_column"]).cast("string"),
                            lit("\n\n"),
                            col(column).cast("string"),
                        ),
                    ).otherwise(col(column)),
                )

        return valid_rows, invalid_rows.drop(*[*validation_columns, *error_columns])

    @abstractmethod
    def validate(self, df: DataFrame, **kwargs) -> tuple[DataFrame, DataFrame]:
        """Validate the input data.

        Args:
            df (DataFrame): The input data to validate.
            kwargs: Additional keyword arguments.

        Returns:
            DataFrame: The validated data.
        """
        pass
