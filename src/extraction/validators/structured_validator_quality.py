from typing import Callable
from pyspark.sql import DataFrame

from extraction.validators.structured_validator_base import ExtractionValidatorBase
from extraction.utils.helper import generate_unique_name

list_rules_type = list[Callable[[str], bool]]


class EntityQualityValidator(ExtractionValidatorBase):
    """Class for validating structured data for quality of the entities."""

    with_unpacking: bool = False

    def __init__(
        self,
        primary_keys: list[str],
        input_columns: list[str] | None = None,
        column_rules: dict[str, list_rules_type] = {},
        with_unpacking: bool = False,
    ) -> None:
        """Initialize the EntityQualityValidator.

        Args:
            primary_keys (list[str]): The primary keys to use for the class.
            input_columns (list[str]): The input columns to use for the class.
            column_rules (dict[str, list[Callable[[str], bool]]): The column rules to use for the class.
            with_unpacking (bool): Whether to unpack the entities.
        """
        super().__init__(
            primary_keys=primary_keys,
            input_columns=input_columns,
            column_rules=column_rules,
        )

        self.with_unpacking = with_unpacking

    def validate_with_unpacking(
        self,
        df: DataFrame,
        entities_to_rules_lookup: dict[str, list_rules_type],
        **kwargs,
    ) -> DataFrame:
        """Validate the structured data for quality of the entities with unpacking.

        Args:
            df (DataFrame): The structured data to validate.
            entities_to_rules_lookup (dict[str, list[Callable[[str], bool]]): The entities to rules lookup.
            kwargs: Additional keyword arguments.
        """
        entities_to_validation_cols_lookup = {
            entity_name: generate_unique_name(
                seed=f"_unpacked_{entity_name}", existing_names=df.columns
            )
            for entity_name in entities_to_rules_lookup.keys()
        }

        validation_cols_to_rules_lookup = {
            entities_to_validation_cols_lookup[entity_name]: rules
            for entity_name, rules in entities_to_rules_lookup.items()
        }

        df = self._unpack_json(
            df=df,
            json_column=self.input_columns[0],
            keys_lookup=entities_to_validation_cols_lookup,
        )

        valid_rows, invalid_rows = self.validate_with_response(
            df=df, column_rules=validation_cols_to_rules_lookup
        )

        invalid_rows = self._pack_json(
            df=invalid_rows,
            json_column=self.input_columns[0],
            keys_lookup={v: k for k, v in entities_to_validation_cols_lookup.items()},
        )

        return valid_rows.drop(
            *entities_to_validation_cols_lookup.values()
        ), invalid_rows.drop(*entities_to_validation_cols_lookup.values())

    def validate(
        self,
        df: DataFrame,
        entities_to_rules_lookup: dict[str, list_rules_type] = {},
        **kwargs,
    ) -> DataFrame:
        """Validate the structured data for quality of the entities.

        Args:
            df (DataFrame): The structured data to validate.
            entities_to_rules_lookup (dict[str, list[Callable[[str], bool]]): The entities to rules lookup.
            kwargs: Additional keyword arguments.
        """
        if self.with_unpacking:
            return self.validate_with_unpacking(df, entities_to_rules_lookup, **kwargs)

        return self.validate_with_response(df=df)
