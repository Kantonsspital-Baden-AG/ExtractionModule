import pytest

from extraction.utils.helper import generate_unique_name


def test_generate_unique_name_seed_not_in_existing_names():
    """Test the generate_unique_name function when the seed is not in the existing names."""
    seed = "column"
    existing_names = ["column_1", "column_2", "column_3"]
    expected = "column"
    msg = "The function should return the seed when it is not in the existing names."
    assert generate_unique_name(seed, existing_names) == expected, msg


def test_generate_unique_name_seed_in_existing_names():
    """Test the generate_unique_name function when the seed is in the existing names."""

    seed = "column"
    existing_names = ["column", "column_1", "column_2", "column_3"]
    expected = "column_"
    msg = "The function should return a unique name when the seed is in the existing names."
    assert generate_unique_name(seed, existing_names) == expected, msg


@pytest.mark.parametrize(
    "max_iter_reached, sep",
    [
        (5, "_"),
        (10, "-"),
    ],
)
def test_generate_unique_name_max_iter_reached(max_iter_reached, sep):
    """Test the generate_unique_name function when the maximum number of iterations is reached."""
    seed = "column"
    existing_names = ["column" + "".join([sep] * i) for i in range(max_iter_reached)]
    with pytest.raises(ValueError):
        generate_unique_name(
            seed, existing_names=existing_names, max_iter=max_iter_reached, sep=sep
        )


@pytest.mark.parametrize("sep", ["_", "."])
def test_generate_unique_name_sep(sep):
    """Test the generate_unique_name function with different separators."""
    seed = "column"
    existing_names = ["column", seed + sep, "column_1", "column_2", "column_3"]
    expected = "column" + sep + sep
    msg = "The function should return a unique name with the correct separator."
    assert (
        generate_unique_name(seed, existing_names, max_iter=5, sep=sep) == expected
    ), msg
