import pytest

from extraction.utils.jsons import filter_json


def test_filter_json_only_dict():
    """Test the filter_json function."""
    input_data = {
        "key1": "value1",
        "key2": "",
        "key3": {
            "key4": "value4",
            "key5": "",
            "key6": {
                "key7": "value7",
                "key8": "",
            },
        },
    }
    expected = {
        "key1": "value1",
        "key3": {
            "key4": "value4",
            "key6": {
                "key7": "value7",
            },
        },
    }
    msg = "The function should filter out empty keys from a multi-nested dictionary."
    assert filter_json(input_data) == expected, msg


def test_filter_json_only_list():
    """Test the filter_json function."""
    input_data = [
        {"key1": "value1"},
        {"key2": ""},
        {"key3": {"key4": "value4"}},
        {"key5": ""},
        {"key6": {"key7": "value7"}},
        {"key8": ""},
    ]

    expected = [
        {"key1": "value1"},
        {"key3": {"key4": "value4"}},
        {"key6": {"key7": "value7"}},
    ]

    msg = "The function should filter out empty keys from a list of dictionaries."
    assert filter_json(input_data) == expected, msg


def test_filter_json_only_str():
    """Test the filter_json function."""
    input_data = "value"
    expected = "value"
    msg = "The function should return the input string when it is not empty."
    assert filter_json(input_data) == expected, msg


def test_filter_json_empty_str():
    """Test the filter_json function."""
    input_data = ""
    expected = ""
    msg = "The function should return an empty string when the input string is empty."
    assert filter_json(input_data) == expected, msg


def test_filter_json_none():
    """Test the filter_json function."""
    input_data = None
    expected = None
    msg = "The function should return None when the input data is None."
    assert filter_json(input_data) == expected, msg


def test_filter_json_invalid_input():
    """Test the filter_json function."""
    input_data = 1
    with pytest.raises(ValueError):
        filter_json(input_data)


def test_filter_json_multinested_json():
    """Test the filter_json function."""
    input_data = {
        "key1": "value1",
        "key2": "",
        "key3": {
            "key4": "value4",
            "key5": "",
            "key6": {
                "key7": "value7",
                "key8": [
                    {"key9": "value9"},
                    {"key10": ""},
                    [{"key11": {"key12": "value12"}}],
                    [{"key13": ""}, {"key14": {"key15": ""}}],
                    {"key13": ""},
                    {"key14": {"key15": "value15"}},
                    "",
                    "extra_value",
                ],
            },
        },
    }
    expected = {
        "key1": "value1",
        "key3": {
            "key4": "value4",
            "key6": {
                "key7": "value7",
                "key8": [
                    {"key9": "value9"},
                    [{"key11": {"key12": "value12"}}],
                    {"key14": {"key15": "value15"}},
                    "extra_value",
                ],
            },
        },
    }
    msg = "The function should filter out empty keys from a multi-nested dictionary."
    assert filter_json(input_data) == expected, msg
