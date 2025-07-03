from typing import Callable
from pyspark.sql import DataFrame

from extraction.validators.structured_validator_quality import EntityQualityValidator
from extraction.validators.structured_validator_structure import (
    StructuralComplianceValidator,
)


list_rules_type = list[Callable[[str], bool]]


class ExtractionValidator:
    """Validate the extracted values from the StructuredExtractor.

    The validator is supposed to validate the following topics:
    - Entity quality: the values extracted from the text are valid entities. This uses the column_rules to validate the values;
    - Structural compliance: the values extracted from the text are compliant with the structure of the input data: i.e. the llm does not extract a value that is not present in the input data;
    - Completeness: the values extracted from the text are complete, i.e. the llm extract all the values present in the input data;
    """

    MAX_CONCURRENT_REQUESTS = 50

    structure_validator: StructuralComplianceValidator = None
    quality_validator: EntityQualityValidator = None

    def __init__(
        self,
        structure_validator: StructuralComplianceValidator,
        quality_validator: EntityQualityValidator | None = None,
    ) -> None:
        """Initialize the ExtractionValidator.

        Args:
            structure_validator (StructuralComplianceValidator): The structure validator.
            quality_validator (EntityQualityValidator): The quality validator.
        """

        self.structure_validator = structure_validator
        self.quality_validator = quality_validator

    def validate_entity_quality(self, df: DataFrame) -> DataFrame:
        """Validate the structured data for quality of the entities.

        Args:
            df (DataFrame): The structured data to validate.

        Returns:
            DataFrame: The valid structured data.
        """
        return self.quality_validator.validate(df)

    def validate_structural_compliance(
        self, df: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """Validate the structured data for compliance with the structure.

        Args:
            df (DataFrame): The structured data to validate.

        Returns:
            tuple[DataFrame, DataFrame]: The valid and invalid structured data.
        """
        return self.structure_validator.validate(df)
