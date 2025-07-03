import asyncio
import json
from typing import Callable, Awaitable
from pyspark.sql import DataFrame
from functools import reduce
from langchain_core.messages import AIMessage, HumanMessage, merge_message_runs
from langchain_core.language_models.chat_models import BaseChatModel

from extraction.entities.extraction_entity import ExtractionEntity
from extraction.extractors.structured_extractor import StructuredExtractor
from extraction.validators.structured_validator import ExtractionValidator
from extraction.validators.structured_validator_quality import EntityQualityValidator
from extraction.validators.structured_validator_structure import (
    StructuralComplianceValidator,
)
from extraction.data_handlers.dataframe_mapper import DataFrameMapper
from extraction.utils.helper import generate_unique_name
from extraction.utils.batcher import generate_batches
from extraction.utils.loggings import logging, logger, set_logger


class StructuredExtraction:
    """Extract defined entities from a given dataframe in free text columns."""

    entities: list[ExtractionEntity] = []
    extractor: StructuredExtractor | None = None
    mapper: DataFrameMapper | None = None
    validator: ExtractionValidator | None = None

    def __init__(
        self, entities: list[ExtractionEntity], _logger: logging.Logger | None = None
    ) -> None:
        """Initialize the StructuredExtraction.

        Args:
            entities (list[ExtractionEntity]): The entities to extract.
        """
        self.entities = self._validate_input_entities(entities)

        if _logger:
            set_logger(_logger)

    @property
    def _is_single_entity_extraction(self) -> bool:
        """Check if the extraction is for a single entity.

        Returns:
            bool: True if the extraction is for a single entity, False otherwise.
        """
        return len(self.entities) == 1

    @property
    def _entities_to_validators_lookup(self) -> dict[str, list[Callable[[str], bool]]]:
        """Return the entities to validators lookup.

        Returns:
            dict[str, list[Callable[[str], bool]]]: The entities to validators lookup.
        """
        return {
            entity.name: rules
            for entity in self.entities
            if (rules := entity.validators)
        }

    def _validate_input_entities(self, entities: list) -> list:
        """Validate the input entities.

        Args:
            entities (list): The list of entities to validate.

        Returns:
            list: The list of entities if they are valid.

        Raises:
            ValueError: If the entities are not of type ExtractionEntity.
        """
        entities_names = set()
        for entity in entities:
            if not isinstance(entity, ExtractionEntity):
                raise ValueError(
                    f"The entity should be of type ExtractionEntity, found: {type(entity)}"
                )

            if entity.name in entities_names:
                raise ValueError(
                    f"Entity name {entity.name} is already defined, please use a unique name."
                )

            entities_names.add(entity.name)

        return entities

    def _setup_validator(self) -> None:
        """Setup the validator for the extraction, if any validation is required from the entities."""

        if not self.entities:
            return

        # Define the StructuralComplianceValidator
        structural_validator_param = {
            "primary_keys": [self.mapper.mapped_primary_key],
            "input_columns": [self.extractor.extracted_column],
        }

        if not self._is_single_entity_extraction:
            structural_validator_param["properties"] = {
                entity.name: {"type": entity.type} for entity in self.entities
            }

        structure_validator = StructuralComplianceValidator(
            **structural_validator_param
        )

        # Define the EntityQualityValidator
        quality_validator = None
        entities_to_validators_lookup = self._entities_to_validators_lookup

        if entities_to_validators_lookup:
            quality_validator_param = {
                "primary_keys": [self.mapper.mapped_primary_key],
                "input_columns": [self.extractor.extracted_column],
                "column_rules": {
                    self.extractor.extracted_column: [
                        lambda s: (
                            r.run_with_response(s)
                            if s is not None
                            else (True, ExtractionValidator.SUCCESS)
                        )
                        for rules in entities_to_validators_lookup.values()
                        for r in rules
                    ]
                },
            }

            if not self._is_single_entity_extraction:
                quality_validator_param["with_unpacking"] = True
                quality_validator_param["column_rules"] = {}

            quality_validator = EntityQualityValidator(**quality_validator_param)

        self.validator = ExtractionValidator(
            structure_validator=structure_validator,
            quality_validator=quality_validator,
        )

    def fit(
        self,
        df: DataFrame,
        primary_keys: list[str],
        input_columns: list[str],
        llm: BaseChatModel,
    ) -> DataFrame:
        """Fit the StructuredExtraction to the input data.

        Args:
            df (DataFrame): The input data to fit the extraction to.
            primary_keys (list[str]): The primary keys to use for the extraction.
            input_columns (list[str]): The columns to extract the entities from.
            llm (BaseChatModel): The language model to use for the extraction.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        if df.isEmpty():
            return df

        try:
            # Define the DataFrameMapper and fit it to the input data
            self.mapper = DataFrameMapper(primary_keys)
            _df_mapped = self.mapper.fit_transform(df)

            # Define the StructuredExtractor and fit it to the input data
            self.extractor = StructuredExtractor(
                llm=llm,
                primary_keys=[self.mapper.mapped_primary_key],
                input_columns=input_columns,
            )

            self.extractor.configure(**self._generate_extractor_configuration())

            # Make sure that the extracted column is not in the input columns
            self.extractor.extracted_column = generate_unique_name(
                seed=self.extractor.extracted_column,
                existing_names=[*primary_keys, *input_columns, *df.columns],
            )

            # Define the ExtractionValidator and fit it to the input data
            self._setup_validator()

            return _df_mapped
        except Exception as e:
            msg = f"Error while fitting the StructuredExtraction: {e}"
            logger.error(msg)
            raise e

    async def _a_extract_loop_back(
        self,
        input_data: DataFrame,
        extracted: DataFrame,
        max_iter: int = 5,
        retry_mesage_prompt: str = "The following records are invalid. Correct them and try again.",
        validation_function: Callable[[DataFrame], tuple[DataFrame, DataFrame]]
        | None = None,
        extraction_function: Callable[[list], Awaitable[DataFrame]] | None = None,
        validation_type: str = "",
    ) -> DataFrame:
        """Handle the loopback to the extractor"""

        extracted, invalid_rows = validation_function(extracted)

        if invalid_rows.count() > 0:
            initial_messages = self.extractor._build_initial_messages(input_data)

            msg = f"Input data: {input_data.collect()}"
            logger.warning(msg)

            iter = 0
            while iter < max_iter and invalid_rows.count() > 0:
                msg = f"{validation_type}Invalid rows found [{iter+1}/{max_iter}]: {invalid_rows.collect()}"
                logger.warning(msg)

                # Construct the messages to send to the language model
                invalid_rows_json = self.extractor._prepare_input_json(
                    invalid_rows,
                    primary_keys=[self.mapper.mapped_primary_key],
                    input_columns=[self.extractor.extracted_column],
                )

                messages = merge_message_runs(
                    initial_messages
                    + [
                        HumanMessage(
                            content=f"""{retry_mesage_prompt}

                        Invalid records:
                        {json.dumps(invalid_rows_json, indent=2, ensure_ascii=False)}
                        """
                        ),
                    ]
                )

                extracted_retry = await extraction_function(messages)
                valid_rows, invalid_rows = validation_function(extracted_retry)

                # merge the valid rows with the previously extracted rows, favor the previously extracted rows
                extracted = extracted.union(
                    valid_rows.join(
                        extracted,
                        on=[self.mapper.mapped_primary_key],
                        how="left_anti",
                    )
                )
                iter += 1

            if iter >= max_iter and invalid_rows.count() > 0:
                msg = f"Maximum number of iterations reached: {max_iter}. Exiting validation loop with {invalid_rows.count()} invalid rows."
                logger.warning(msg)

        return extracted

    async def _handle_structural_compliance_validation(
        self,
        input_data: DataFrame,
        extracted: DataFrame,
        max_iter: int = 3,
    ) -> DataFrame:
        """Handle the structural compliance validation.

        Args:
            input_data (DataFrame): The input data.
            extracted (DataFrame): The extracted data.
            max_iter (int): The maximum number of iterations to retry the extraction.

        Returns:
            DataFrame: The validated DataFrame.
        """
        return await self._a_extract_loop_back(
            input_data=input_data,
            extracted=extracted,
            max_iter=max_iter,
            retry_mesage_prompt="""The following records have been extracted with the wrong structure:
            either the identifier is missing from the input data or the extracted entities do not match the expected format.
            Correct them and try again.""",
            validation_function=lambda e: self.validator.structure_validator.validate(
                df=e, input_data=input_data
            ),
            extraction_function=lambda m: self.extractor.a_extract_batch_structured(
                messages=m
            ),
            validation_type="[struct-val] ",
        )

    async def _handle_entity_quality_validation(
        self,
        input_data: DataFrame,
        extracted: DataFrame,
        max_iter: int = 3,
    ) -> DataFrame:
        """Handle the entity quality validation.

        Args:
            input_data (DataFrame): The input data.
            extracted (DataFrame): The extracted data.
            max_iter (int): The maximum number of iterations to retry the extraction.

        Returns:
            DataFrame: The validated DataFrame.
        """

        async def extraction_with_structural_compliance_validation(
            m: list[AIMessage],
        ) -> DataFrame:
            extracted = await self.extractor.a_extract_batch_structured(messages=m)
            return await self._handle_structural_compliance_validation(
                input_data, extracted
            )

        return await self._a_extract_loop_back(
            input_data=input_data,
            extracted=extracted,
            max_iter=max_iter,
            retry_mesage_prompt="""The following records have been wrongly extracted: each invalid record will be shown with the extracted error.
            If no error is shown in part of the record, then that part of the record is valid and should be kept.
            If no meaningful error is shown, then please check the format of the extracted entities.
            Correct the invalid entities.""",
            validation_function=lambda e: self.validator.quality_validator.validate(
                df=e,
                entities_to_rules_lookup=self._entities_to_validators_lookup,
            ),
            extraction_function=extraction_with_structural_compliance_validation,
            validation_type="[entity-val] ",
        )

    async def _a_extract_batch_with_validation(self, df: DataFrame) -> DataFrame:
        """Extract the entities from the input data.

        Args:
            df (DataFrame): The input data to extract the entities from.

        Returns:
            DataFrame: The extracted DataFrame.
        """
        msg = "Extracting entities"
        logger.info(msg)
        # Extract the entities
        extracted = await self.extractor.a_extract_batch_structured(df=df)

        msg = "Validating structure"
        logger.info(msg)
        # Validate the structural compliance
        extracted = await self._handle_structural_compliance_validation(df, extracted)

        if self.validator.quality_validator:
            msg = "Validating entities"
            logger.info(msg)
            # Validate the entity quality
            extracted = await self._handle_entity_quality_validation(df, extracted)

        msg = "Done"
        logger.info(msg)

        return extracted

    async def _a_extract_batch(self, df: DataFrame) -> DataFrame:
        """Extract the entities from the input data.

        Args:
            df (DataFrame): The input data to extract the entities from.

        Returns:
            DataFrame: The extracted DataFrame.
        """
        if self.validator:
            return await self._a_extract_batch_with_validation(df)

        return await self.extractor.a_extract_batch_structured(df=df)

    async def _a_extract_all(
        self, df: DataFrame, batch_size: int | None = None, limit: int | None = None
    ) -> DataFrame:
        """Extract the entities from the input data.

        Args:
            df (DataFrame): The input data to extract the entities from.
            batch_size (int): The batch size for the extraction.
            limit (int | None): The limit for the extraction.

        Returns:
            DataFrame: The extracted DataFrame.
        """
        dfs = await asyncio.gather(
            *[
                self._a_extract_batch(batch)
                for batch in generate_batches(df, batch_size=batch_size, limit=limit)
            ]
        )

        if len(dfs) == 1:
            return dfs[0]

        return reduce(lambda x, y: x.union(y), dfs)

    def extract(
        self, df: DataFrame, batch_size: int | None = None, limit: int | None = None
    ) -> DataFrame:
        """Extract the entities from the input data.

        Args:
            df (DataFrame): The input data to extract the entities from.
            batch_size (int): The batch size for the extraction.
            limit (int | None): The limit for the extraction.

        Returns:
            DataFrame: The extracted DataFrame.
        """
        if df.isEmpty():
            return df

        try:
            extracted_values = asyncio.run(self._a_extract_all(df, batch_size, limit))

            return self.mapper.transform_inverse(extracted_values)
        except Exception as e:
            msg = f"Error while extracting: {e}"
            logger.error(msg)
            raise e

    def fit_extract(
        self,
        df: DataFrame,
        llm: BaseChatModel,
        primary_keys: list[str],
        input_columns: list[str],
        batch_size: int | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        """Fit the StructuredExtraction to the input data and extract the entities.

        Args:
            df (DataFrame): The input data to fit the extraction to.
            llm (BaseChatModel): The language model to use for the extraction.
            primary_keys (list[str]): The primary keys to use for the extraction.
            input_columns (list[str]): The columns to extract the entities from.
            batch_size (int): The batch size for the extraction.
            limit (int | None): The limit for the extraction.

        Returns:
            DataFrame: The extracted DataFrame.
        """
        if df.isEmpty():
            return df

        df_mapped = self.fit(
            df=df, llm=llm, primary_keys=primary_keys, input_columns=input_columns
        )
        return self.extract(df_mapped, batch_size=batch_size, limit=limit)

    def _generate_extractor_configuration(self) -> dict:
        """Generate the extractor configuration.

        Returns:
            dict: The extractor configuration.
        """
        initial_prompt = """You are a medical, entity extraction expert.
        I will show you a batch of records in the format of {<id>: <content>}
        and your goal is to extract the entities from the records."""

        try:
            prompt = (
                initial_prompt
                + "\n ENTITIES:\n"
                + "\n".join(
                    [
                        f" - {entity.name}: {entity.instructions}"
                        for entity in self.entities
                    ]
                )
            )

            context = "\n".join(
                [
                    f"{entity.name}: {entity.context}"
                    for entity in self.entities
                    if entity.context
                ]
            )

            if context:
                context = f"CONTEXT:\n {context}"

            extracted_value_format = "<extracted_value>"
            if not self._is_single_entity_extraction:
                extracted_value_format = json.dumps(
                    {f"{entity.name}": "<extracted_value>" for entity in self.entities},
                    indent=2,
                    ensure_ascii=False,
                )

            format = f"""Respond in the following JSON format:
    {{"<id>": {extracted_value_format}}}
    Do not add anything more than the JSON, containing only the identifiers and the extracted entities.
    Make sure to use the identifiers from the input data: do not change them."""

        except Exception as e:
            msg = f"Error while generating the extractor configuration: {e}"
            logger.error(msg)
            raise e

        return {"prompt": prompt, "context": context, "format": format}
