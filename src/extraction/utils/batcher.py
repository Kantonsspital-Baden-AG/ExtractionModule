from typing import Generator
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, monotonically_increasing_id, row_number

from extraction.utils.helper import generate_unique_name


def generate_batches(
    df: DataFrame, batch_size: int | None = None, limit: int | None = None
) -> Generator[DataFrame, None, None]:
    """Generate batches of a DataFrame.

    Args:
        df (DataFrame): The DataFrame to generate batches from.
        batch_size (int): The size of the batches to generate.
        limit (int): The maximum number of rows to generate batches for.

    Yields:
        Generator[DataFrame]: A DataFrame batch.
    """

    df_count = df.count()
    if df_count == 0:
        yield df
        return

    if not limit:
        limit = df_count

    if not batch_size:
        batch_size = df_count

    limit = min(limit, df_count)
    batch_size = min(batch_size, limit)

    if batch_size == df_count and limit == df_count:
        yield df
        return

    row_id_name = generate_unique_name(seed="_row_id_", existing_names=df.columns)
    window = Window.orderBy(monotonically_increasing_id())
    df = df.withColumn(row_id_name, row_number().over(window))

    for i in range(0, limit, batch_size):
        batch_limit = min(batch_size, limit - i)

        yield df.filter(col(row_id_name).between(i + 1, i + batch_limit)).drop(
            row_id_name
        )
