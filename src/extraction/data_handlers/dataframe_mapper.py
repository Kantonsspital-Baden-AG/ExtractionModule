from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from extraction.data_handlers.excel_indexer import ExcelIdentifierGenerator
from extraction.data_handlers.structured_class import StructuredDataHandler
from extraction.utils.loggings import logger
from extraction.utils.helper import generate_unique_name


class DataFrameMapper(StructuredDataHandler):
    """Class to map a dataframe with primary keys into a dataframe with a sigle column of primary keys consisting of
    an excel-like identifier.
    """

    concordance_table: dict
    primary_keys: list[str]
    id_gen: ExcelIdentifierGenerator = ExcelIdentifierGenerator()
    mapped_primary_key: str = None

    def __init__(self, primary_keys: list[str]):
        super().__init__(primary_keys=primary_keys, input_columns=[])

        self.id_gen = ExcelIdentifierGenerator()

    def fit(self, input_data: DataFrame) -> None:
        """Fit the DataFrameMapper to the input data.

        Args:
            input_data (DataFrame): The input data to fit the mapper to.
        """
        try:
            self.concordance_table = self._generate_concordance_table(input_data)
            self.mapped_primary_key = generate_unique_name(
                seed="_primary_key_", existing_names=input_data.columns
            )
        except Exception as e:
            msg = f"Error fitting DataFrameMapper: {e}"
            logger.error(msg)
            raise Exception(msg)

    def transform(self, input_data: DataFrame) -> DataFrame:
        """Transform the input data.

        Args:
            input_data (DataFrame): The input data to transform.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        try:
            concordance_table_df = self._convert_concordance_table_to_df()

            not_primary_columns = [
                c for c in input_data.columns if c not in self.primary_keys
            ]

            # Join the input DataFrame with the concordance table
            return input_data.join(
                concordance_table_df, on=self.primary_keys, how="inner"
            ).select(self.mapped_primary_key, *not_primary_columns)
        except Exception as e:
            msg = f"Error transforming DataFrameMapper: {e}"
            logger.error(msg)
            raise Exception(msg)

    def fit_transform(self, input_data: DataFrame) -> DataFrame:
        """Fit the DataFrameMapper to the input data and transform the input data.

        Args:
            input_data (DataFrame): The input data to fit the mapper to.

        Returns:
            DataFrame: The mapped DataFrame.
        """
        self.fit(input_data)
        return self.transform(input_data)

    def _generate_concordance_table(self, input_data: DataFrame) -> DataFrame:
        """Generate a lookup table for the primary keys in the form {"A": "<primary_keys1>", "B": "<primary_keys2>", ...}

        Args:
            input_data (DataFrame): The input data to generate the concordance table from.

        Returns:
            DataFrame: The concordance table.
        """
        return {
            self.id_gen.generate_next(): {
                column: row[column] for column in self.primary_keys
            }
            for row in input_data.collect()
        }

    def _convert_concordance_table_to_df(self) -> DataFrame:
        """Convert the concordance table to a DataFrame.

        Returns:
            DataFrame: The concordance table DataFrame.
        """
        try:
            return SparkSession.builder.getOrCreate().createDataFrame(
                [
                    {self.mapped_primary_key: key, **values}
                    for key, values in self.concordance_table.items()
                ]
            )
        except Exception as e:
            msg = f"Error converting concordance table to DataFrame: {e}"
            logger.error(msg)
            raise Exception(msg)

    def transform_inverse(self, input_data: DataFrame) -> DataFrame:
        """Transform the input data back to the original form.

        Args:
            input_data (DataFrame): The input data to transform.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        try:
            concordance_table_df = self._convert_concordance_table_to_df()

            not_primary_columns = [
                c for c in input_data.columns if c != self.mapped_primary_key
            ]
            return input_data.join(
                concordance_table_df, on=self.mapped_primary_key, how="inner"
            ).select(*self.primary_keys, *not_primary_columns)
        except Exception as e:
            msg = f"Error transforming DataFrameMapper inverse: {e}"
            logger.error(msg)
            raise Exception(msg)


# Usage example
#
# from dataframe_mapper import DataFrameMapper
# from pyspark.sql import SparkSession
#
# input_data = spark.createDataFrame(
#     [
#         {"PatientID": 1, "Date": "2021-01-01", "Value": 10},
#         {"PatientID": 2, "Date": "2021-01-02", "Value": 20},
#         {"PatientID": 3, "Date": "2021-01-03", "Value": 30},
#     ]
# )
#
# # Define the primary keys
# primary_keys = ["PatientID", "Date"]
#
# # Create the DataFrameMapper
# mapper = DataFrameMapper()
#
# # Fit and transform the input data
# transformed_data = mapper.fit_transform(input_data, primary_keys)
#
# # Transform the input data back to the original form
# original_data = mapper.transform_inverse(transformed_data)
#
