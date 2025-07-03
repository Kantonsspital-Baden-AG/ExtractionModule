from typing import Generator, Union


def filter_non_empty_keys(
    input: Union[dict, str, list],
) -> Generator[Union[dict, str, list], None, None]:
    """Filter out empty keys from a dictionary, string or list.

    Args:
        input (Union[dict, str, list]): The input data.

    Yields:
        Union[dict, str, list]: The filtered data.
        dict|str|list: The filtered data.
    """
    if isinstance(input, dict):
        for key, value in input.items():
            if filter_non_empty_keys(value):
                yield key, value

        return

    if isinstance(input, list):
        for value in input:
            if filter_non_empty_keys(value):
                yield value

        return

    if isinstance(input, str):
        if input:
            yield input

        return

    raise ValueError(
        f"The input data must be a dictionary, string or list. The following input data is not supported: {input}"
    )


def filter_json(input_data: Union[dict, list, str, None]) -> Union[dict, list, str]:
    """Filter out empty keys from a multi-nested dictionary.

    Example:
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
    filtered_data = filter_json(input_data) =
    {
        "key1": "value1",
        "key3": {
            "key4": "value4",
            "key6": {
                "key7": "value7",
            },
        },
    }

    Args:
        input_data (Union[dict, list, str]): The input data.

    Returns:
        Union[dict, list,str]: The filtered data.
    """
    if input_data is None:
        return None

    if isinstance(input_data, dict):
        return {
            key: value_filered
            for key, value in input_data.items()
            if (value_filered := filter_json(value))
        }

    if isinstance(input_data, list):
        return [
            value_filered
            for value in input_data
            if (value_filered := filter_json(value))
        ]

    if isinstance(input_data, str):
        return input_data

    raise ValueError(
        f"The input data must be a dictionary, list or string. Input data: {input_data}"
    )
