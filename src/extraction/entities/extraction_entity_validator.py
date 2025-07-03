from typing import Callable


class ExtractionEntityValidator:
    SUCCESS = "<Success>"

    name: str  # The name of the validator.
    func: Callable[[str], bool]  # The function to validate the entity.
    error_msg: str  # The error message to display if the validation fails.
    description: str  # The description of the validator.

    def __init__(
        self,
        name: str,
        func: Callable[[str], bool],
        error_msg: str | None = None,
        description: str | None = None,
    ) -> None:
        """Initialize the Validator object.

        Args:
            name (str): The name of the validator.
            validator (Callable[[str], bool]): The function to validate the entity.
            error_msg (str): The error message to display if the validation fails.
            description (str): The description of the validator.
        """
        self.name = name
        self.func = func
        self.error_msg = error_msg if error_msg else f"Validation failed for {name}"
        self.description = description if description else f"Validator for {name}"

    def __repr__(self) -> str:
        return f"{self.name}: {self.description}"

    def run(self, value: str) -> bool:
        """Run the validator on the value.

        Args:
            value (str): The value to validate.

        Returns:
            bool: True if the value is valid, False otherwise.
        """
        return self.func(value)

    def run_with_response(self, value: str) -> bool:
        """Run the validator on the value and return the error message if the validation fails.

        Args:
            value (str): The value to validate.

        Returns:
            bool: True if the value is valid, False otherwise.
        """
        is_valid = self.func(value)
        return is_valid, self.error_msg if not is_valid else self.SUCCESS
