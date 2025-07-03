from extraction.data_handlers.excel_indexer import ExcelIdentifierGenerator


def test_excel_indexer():
    """Test ExcelIdentifierGenerator"""
    excel_indexer = ExcelIdentifierGenerator()

    for i, id in enumerate(excel_indexer):
        if i == 0:
            assert id == "A"

        if i == 25:
            assert id == "Z"

        if i == 26:
            assert id == "AA"

        if i == 27:
            assert id == "AB"

        if i == 100:
            assert id == "CW"

        if i == 2000:
            assert id == "BXY"

        if i == 2500:
            assert id == "CRE"
            break
