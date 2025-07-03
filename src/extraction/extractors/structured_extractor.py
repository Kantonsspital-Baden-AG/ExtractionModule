import asyncio
import nest_asyncio
import re
import json
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.language_models.chat_models import BaseChatModel

from extraction.utils.loggings import logger
from extraction.utils.jsons import filter_json
from extraction.utils.batcher import generate_batches
from extraction.data_handlers.structured_class import StructuredDataHandler

nest_asyncio.apply()


class StructuredExtractor(StructuredDataHandler):
    """Extract structured data from a DataFrame containing a free text columns."""

    MAX_CONCURRENT_REQUESTS = 50

    primary_keys: list[str]
    input_columns: list[str]

    llm: BaseChatModel
    _prompt: str

    extracted_column: str = "_extracted_values_"

    def __init__(
        self,
        llm: BaseChatModel,
        primary_keys: list[str],
        input_columns: list[str],
    ) -> None:
        """Initialize the StructuredExtractor object.

        Args:
            llm (BaseChatModel): The language model to use for extracting the structured data.
            primary_keys (list[str]): The primary keys of the input DataFrame.
            input_columns (list[str]): The columns to use as input for the language model.
        """
        super().__init__(primary_keys=primary_keys, input_columns=input_columns)
        self.llm = llm

    @property
    def prompt(self) -> str:
        """Get the prompt to use for the language model."""
        return f"""{self._prompt}.

{self.context if self.context else ''}

{self.format}
"""

    def configure(
        self, prompt: str, context: str = "", format: str | None = None
    ) -> None:
        """Configure the extraction process by providing the prompt and context.

        Args:
            prompt (str): The prompt to use for the language model. This part is often referred as the Instruction part of the prompt.
            context (str): The context to use for the language model.
        """
        self._prompt = prompt
        self.context = context

        if not format:
            identifiers_string = ",".join([f"<{c}>" for c in self.primary_keys])
            if len(self.primary_keys) > 1:
                identifiers_string = f"({identifiers_string})"

            format = f"""Respond in the following JSON format:
{{ "{identifiers_string}": <extracted_value> }}
Do not add anything more than the JSON."""

        self.format = format

    async def _a_get_llm_reponse(self, messages: list[AIMessage]) -> dict:
        """Extract from the input df using the language model.

        Args:
            messages (list[AIMessage]): The messages to send to the language model.

        Returns:
            dict: The extracted data in the form
                    {"(<primary_keys>)": "extracted_value"}
        """
        try:
            async with asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS):
                return await self.llm.ainvoke(messages)

        except Exception as e:
            msg = f"Error invoking the language model: {e}"
            logger.error(msg)
            raise Exception(msg)

    def _parse_llm_response(self, response: AIMessage) -> dict:
        """Parse the response from the language model.
        It is assumed that the response contains a JSON format, and only the JSON format is extracted.
        If the response contains multiple JSON formats, only the first one is extracted.
        If the response does not contain a JSON format, an exception is raised.

        Args:
            response (str): The response from the language model.

        Returns:
            dict: The extracted data in the form
                    {"(<primary_keys>)": "extracted_value"}
        """
        pattern = r"\{(?:[^{}]*|\{(?:[^{}]*|\{[^{}]*\})*\})*\}"
        # Explain the regex
        # \{ matches the opening curly brace
        # (?: starts a non-capturing group
        # [^{}]* matches any character except curly braces, 0 or more times
        # | means OR
        # \{(?:[^{}]*|\{[^{}]*\})*\} matches nested curly braces
        # \} matches the closing curly brace

        match = re.search(pattern, response.content, re.DOTALL)

        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                raise Exception(f"Invalid JSON: {match.group(0)}")

        raise Exception(f"Cannot find JSON format in response: {response.content}")

    async def _a_get_structured_llm_response(
        self, messages: list[AIMessage], max_iter: int = 5
    ) -> dict:
        """Get the structured response from the language model.
        If the LLM does not return a structured response, it will retry with the same input.

        Args:
            messages (list[AIMessage]): The messages to send to the language model.
            max_iter (int): The maximum number of iterations to retry.
        """
        iter = 0
        while iter < max_iter:
            try:
                response = await self._a_get_llm_reponse(messages=messages)

                if response is None:
                    raise Exception("Response is None.")

            except Exception as e:
                msg = f"Error getting response from llm [iteration {iter+1}|{max_iter}]: {e}. Retrying..."
                logger.warning(msg)

                iter += 1
                continue

            try:
                return self._parse_llm_response(response)
            except Exception as e:
                msg = f"Error parsing response from llm [iteration {iter+1}|{max_iter}]: {e}. Retrying..."
                logger.warning(msg)

                messages.append(AIMessage(content=response.content))
                messages.append(
                    HumanMessage(
                        content=f"The response is not in the expected format: {e.__str__()}. Retry."
                    )
                )

            iter += 1

        raise Exception(f"Cannot get structured response after {max_iter} iterations.")

    def _build_initial_messages(
        self,
        df: DataFrame,
        primary_keys: list[str] | None = None,
        input_columns: list[str] | None = None,
    ) -> list[AIMessage]:
        """Build the initial messages to send to the language model.

        Args:
            df (DataFrame): The input DataFrame.
            primary_keys (list[str]|None): The primary keys to use for the input JSON.
            input_columns (list[str]|None): The input columns to use for the input JSON.

        Returns:
            list[AIMessage]: The initial messages.
        """
        if not primary_keys:
            primary_keys = self.primary_keys

        if not input_columns:
            input_columns = self.input_columns

        return [
            SystemMessage(content=self.prompt),
            HumanMessage(
                content=f"Input:\n {json.dumps(self._prepare_input_json(df, primary_keys=primary_keys, input_columns=input_columns), indent=4, ensure_ascii=False)}"
            ),
        ]

    async def _a_extract_batch_json(self, messages: list | None = None) -> dict:
        """Extract the structured data from the input DataFrame.

        Args:
            messages (list): The messages to send to the language model.

        Returns:
            dict: The extracted data in the form {"(<primary_keys>)": "extracted_value"}
        """
        extracted_json = await self._a_get_structured_llm_response(messages=messages)

        # Filtering and cleanup the extracted JSON
        return {
            k: v if isinstance(v, str) else json.dumps(v, indent=4, ensure_ascii=False)
            for k, v in filter_json(extracted_json).items()
        }

    async def a_extract_batch_structured(
        self, df: DataFrame | None = None, messages: list | None = None
    ) -> DataFrame:
        """Extract the structured data from the input DataFrame.

        Args:
            df (DataFrame): The input DataFrame.
            messages (list): The messages to send to the language model.

        Returns:
            DataFrame: The extracted structured data.
        """
        if not messages:
            messages = self._build_initial_messages(df)

        extracted_json = await self._a_extract_batch_json(messages=messages)

        schema = StructType(
            [
                StructField(name, StringType(), True)
                for name in [*self.primary_keys, self.extracted_column]
            ]
        )

        # Use the df session to create the DataFrame

        return SparkSession.builder.getOrCreate().createDataFrame(
            [
                (*key.replace("(", "").replace(")", "").split(","), value)
                for key, value in extracted_json.items()
            ],
            schema,
        )

    async def a_extract_all(
        self,
        input_data: DataFrame,
        batch_size: int | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        """Extract the structured data from the input DataFrame in batches.

        Args:
            input_data (DataFrame): The input dataframe.
            batch_size (int): The size of the batch.
            limit (int|None): The limit of the number of records to process.

        Returns:
            DataFrame: The extracted structured data.
        """
        dfs = await asyncio.gather(
            *[
                self.a_extract_batch_structured(df=batch)
                for batch in generate_batches(
                    input_data, batch_size=batch_size, limit=limit
                )
            ]
        )

        if len(dfs) == 1:
            return dfs[0]

        return reduce(lambda x, y: x.union(y), dfs)

    def run(
        self,
        input_data: DataFrame,
        batch_size: int | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        """Run the extraction process.

        Args:
            input_data (DataFrame): The input dataframe
            batch_size (int): The size of the batch.
            limit (int|None): The limit of the number of records to process.

        Returns:
            DataFrame: The extracted structured data.
        """
        return asyncio.run(
            self.a_extract_all(
                input_data=input_data, batch_size=batch_size, limit=limit
            )
        )
