import json
from jsonschema import validate, ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat

from extraction.validators.structured_validator_base import ExtractionValidatorBase


class StructuralComplianceValidator(ExtractionValidatorBase):
    """Class for validating structured data for compliance with the structure."""

    properties: dict[str, dict[str, str]]
    required: list[str]
    has_additional_properties: bool = False

    def __init__(
        self,
        primary_keys: list[str],
        input_columns: list[str] | None = None,
        properties: dict[str, dict[str, str]] = {},
        required: list[str] = [],
        has_additional_properties: bool = False,
    ) -> None:
        """Initialize the StructuralComplianceValidator.

        Example of a schema
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"},
            },
            "required": ["name", "age"],
            "additionalProperties": False
        }

        -> The schema above defines a JSON object with two properties: name and age.
           The name property must be a string, and the age property must be a number.
           Both properties are required, and no additional properties are allowed.

        Args:
            primary_keys (list[str]): The primary keys to use for the class.
            input_columns (list[str]): The input columns to use for the class.
            properties (dict[str, dict[str,str]]): The properties of the extracted values.
            required (list[str]): The required properties of the extracted values.
            has_additional_properties (bool): Whether the extracted values can have additional properties.
        """
        super().__init__(primary_keys=primary_keys, input_columns=input_columns)

        self.properties = properties
        self.required = required
        self.has_additional_properties = has_additional_properties

    @property
    def schema(self) -> dict:
        return {
            "type": "object",
            "properties": self.properties,
            "required": self.required,
            "additionalProperties": self.has_additional_properties,
        }

    def _validate_extracted_primary_keys(
        self,
        input_data: DataFrame,
        output_data: DataFrame,
        primary_keys: list[str] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
        """Validate the input data for compliance with the structure.

        Args:
            input_data (DataFrame): The input data to validate.
            output_data (DataFrame): The output data to validate.
            primary_keys (list[str]): The primary keys to use for the validation.

        Returns:
            DataFrame: The validated data.
        """
        if not primary_keys:
            primary_keys = self.primary_keys

        # Validate the primary keys
        # Find the missing values in the output data compared to the input data
        invalid_rows = output_data.join(input_data, on=primary_keys, how="left_anti")

        # Filter the invalid rows from the output data
        valid_rows = output_data.join(invalid_rows, on=primary_keys, how="left_anti")

        return valid_rows, invalid_rows

    def _run_extracted_structure_validation(self, input_data: str) -> tuple[bool, str]:
        """Run the validator on the value and return the error message if the validation fails.

        Args:
            input_data (str): The value to validate.

        Returns:
            bool: True if the value is valid, False otherwise.
        """
        try:
            input_data = json.loads(input_data, strict=False)
        except Exception as e:
            return (False, f"Invalid JSON format: {e.message}")

        try:
            validate(instance=input_data, schema=self.schema)
            return (True, self.SUCCESS)
        except ValidationError as e:
            return (False, f"{e.message}")

    def validate(
        self,
        df: DataFrame,
        input_data: DataFrame,
        primary_keys: list[str] | None = None,
        input_columns: list[str] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
        """Validate the input data for compliance with the structure.

        Args:
            df (DataFrame): The data to validate.
            input_data (DataFrame): The input data that produced the df data.
            primary_keys (list[str]): The primary keys to use for the validation.
            input_columns (list[str]): The input columns to use for the validation.

        Returns:
            tuple[DataFrame, DataFrame]: The valid and invalid data.
        """
        if not primary_keys:
            primary_keys = self.primary_keys

        if not input_columns:
            input_columns = self.input_columns

        valid_rows, invalid_rows = self._validate_extracted_primary_keys(
            input_data=input_data, output_data=df, primary_keys=primary_keys
        )

        for column in input_columns:
            invalid_rows = invalid_rows.withColumn(
                column,
                concat(
                    lit("Invalid primary key: primary key not found in input data"),
                    lit("\n\n"),
                    col(column).cast("string"),
                ),
            )

        # If the properties of the extracted values are defined, validate them
        if self.properties:
            valid_rows, new_invalid_rows = self.validate_with_response(
                df=valid_rows,
                primary_keys=primary_keys,
                input_columns=input_columns,
                column_rules={
                    col_name: [self._run_extracted_structure_validation]
                    for col_name in input_columns
                },
            )

            invalid_rows = invalid_rows.union(new_invalid_rows)

        return valid_rows, invalid_rows
