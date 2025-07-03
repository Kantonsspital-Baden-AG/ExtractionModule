def generate_unique_name(
    seed: str = "column",
    existing_names: list = [],
    max_iter: int = 1000,
    sep: str = "_",
) -> str:
    """Generate a unique name by appending a number to the seed.

    Args:
        seed (str, optional): The seed name. Defaults to "column".
        existing_names (list, optional): A list of existing names. Defaults to [].
        max_iter (int, optional): The maximum number of iterations. Defaults to 1000.
        sep (str, optional): The separator between the seed and the number. Defaults to "_".

    Returns:
        str: A unique name.
    """
    if seed not in existing_names:
        return seed

    iter = 0
    while (seed in existing_names) and (iter < max_iter):
        seed += sep
        iter += 1

    if iter >= max_iter:
        raise ValueError("The maximum number of iterations has been reached.")

    return seed
