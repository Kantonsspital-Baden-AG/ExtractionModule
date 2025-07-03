import pytest

from extraction.entities.extraction_entity import ExtractionEntity
from extraction.entities.extraction_entity_validator import ExtractionEntityValidator


def test_add_validator():
    entity = ExtractionEntity(
        name="test_entity",
        instructions="Test instructions",
    )

    entity.add_validator(
        ExtractionEntityValidator(
            func=lambda x: x == "test",
            error_msg="Value should be 'test'",
            name="Test validator",
            description="Test description",
        )
    )

    assert len(entity.validators) == 1
    assert entity.validators[0].name == "Test validator"
    assert entity.validators[0].description == "Test description"
    assert entity.validators[0].run("test")
    assert not entity.validators[0].run("test1")
    assert entity.validators[0].run_with_response("test1") == (
        False,
        "Value should be 'test'",
    )
    assert entity.validators[0].run_with_response("test") == (
        True,
        entity.validators[0].SUCCESS,
    )

    entity.add_validator(
        ExtractionEntityValidator(
            func=lambda x: x == "test1",
            error_msg="Value should be 'test1'",
            name="Test validator 1",
            description="Test description 1",
        )
    )

    assert len(entity.validators) == 2
    assert entity.validators[1].name == "Test validator 1"
    assert entity.validators[1].description == "Test description 1"
    assert entity.validators[1].run("test1")
    assert not entity.validators[1].run("test")
    assert entity.validators[1].run_with_response("test") == (
        False,
        "Value should be 'test1'",
    )
    assert entity.validators[1].run_with_response("test1") == (
        True,
        entity.validators[1].SUCCESS,
    )

    entity.add_validator(
        ExtractionEntityValidator(
            func=lambda x: x == "test2",
            error_msg="Value should be 'test2'",
            name="Test validator 2",
            description="Test description 2",
        )
    )

    assert len(entity.validators) == 3
    assert entity.validators[2].name == "Test validator 2"
    assert entity.validators[2].description == "Test description 2"
    assert entity.validators[2].run("test2")
    assert not entity.validators[2].run("test") and not entity.validators[2].run(
        "test1"
    )
    assert entity.validators[2].run_with_response("test") == (
        False,
        "Value should be 'test2'",
    )
    assert entity.validators[2].run_with_response("test1") == (
        False,
        "Value should be 'test2'",
    )


def test_invalid_validator():
    with pytest.raises(ValueError):
        ExtractionEntity(
            name="test_entity",
            instructions="Test instructions",
            validators=["test_validator"],
        )


def test_entity_no_validator():
    entity = ExtractionEntity(
        name="test_entity",
        instructions="Test instructions",
    )

    assert entity.name == "test_entity"
    assert entity.instructions == "Test instructions"
    assert entity.context is None
    assert entity.validators == []
