from extraction.entities.extraction_entity_validator import ExtractionEntityValidator


class ExtractionEntity:
    """Entity to be extracted from a dataframe column(s) containing free text."""

    name: str  # The name of the entity.
    instructions: str  # The instructions to provide to the user.
    context: str  # The context to provide to the user.
    type: str = "string"  # The type of the validator.

    validators: list[
        ExtractionEntityValidator
    ]  # The validators to apply to the entity.

    def __init__(
        self,
        name: str,
        instructions: str,
        context: str | None = None,
        type: str = "string",
        validators: ExtractionEntityValidator
        | list[ExtractionEntityValidator]
        | None = None,
    ) -> None:
        """Initialize the Entity object.

        Args:
            name (str): The name of the entity.
            instructions (str): The instructions to provide to the user.
            context (str): The context to provide to the user.
        """
        self.name = name.replace(" ", "_")
        self.instructions = instructions
        self.context = context
        self.type = type

        self.validators = (
            self._validate_input_validators(validators) if validators else []
        )

    def _validate_validator(
        self, validator: ExtractionEntityValidator
    ) -> ExtractionEntityValidator:
        """Validate the input validator.

        Args:
            validator (ExtractionEntityValidator): The validator to validate.

        Returns:
            ExtractionEntityValidator: The validator if it is valid.

        Raises:
            ValueError: If the validator is not of type ExtractionEntityValidator.
        """
        if not isinstance(validator, ExtractionEntityValidator):
            raise ValueError(
                f"The validator should be of type ExtractionEntityValidator, found: {type(validator)}"
            )
        return validator

    def _validate_input_validators(
        self, validators: ExtractionEntityValidator | list[ExtractionEntityValidator]
    ) -> list:
        """Validate the input validators.

        Args:
            validators (list): The list of validators to validate.

        Returns:
            list: The list of validators if they are valid.

        Raises:
            ValueError: If the validators are not of type ExtractionEntityValidator.
        """
        if not isinstance(validators, list):
            validators = [validators]

        for validator in validators:
            self._validate_validator(validator)

        # Validate that the validators are unique.
        validators_names = []
        for validator in validators:
            if validator.name in validators_names:
                raise ValueError(
                    f"Validator with name {validator.name} already exists, please use a different name."
                )
            validators_names.append(validator.name)

        return validators

    def add_validator(self, validator: ExtractionEntityValidator) -> None:
        """Add a validator to the entity.

        Args:
            validator (ExtractionEntityValidator): The validator to add.
        """
        self.validators.append(self._validate_validator(validator))

        self.validators = self._validate_input_validators(self.validators)


# Usage
# from structured_extractor import ExtractionEntity
#
# explanation = """The TNM code is used to describe the stage of cancer based on the size and extent of the primary tumor (T),
# the number of nearby lymph nodes that have cancer (N),
# and the presence of metastasis (M)."""
#
# entityTNM = ExtractionEntity(
#     name="TNM",
#     instructions="""The extracted TNM code should have space-separated values for T, N and M, including prefixes if available.
# If partial TNM code is provided, then extract the values that are provided.
# If the record does not include a TNM code, do not extract anything.""",
#     context=explanation,
# )
