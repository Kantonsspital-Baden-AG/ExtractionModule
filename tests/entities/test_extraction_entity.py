from extraction.entities.extraction_entity import ExtractionEntity


def test_extraction_entity_no_validation():
    entity = ExtractionEntity(
        name="test_entity",
        instructions="Test instructions",
        context="Test context",
        type="string",
    )

    assert entity.name == "test_entity"
    assert entity.instructions == "Test instructions"
    assert entity.context == "Test context"
    assert entity.validators == []
    assert entity.type == "string"
