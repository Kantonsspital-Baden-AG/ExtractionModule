from pathlib import Path
from langchain_ollama import ChatOllama

import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from extraction.extraction import StructuredExtraction
from extraction.entities.extraction_entity import ExtractionEntity

# Define the LLM to use
llm = ChatOllama(
    model="llama3",
    temperature=0.0,
)

# Load the data
file_path = Path("./data/ExtractionData.csv")
spark = SparkSession.builder.appName("Extraction").getOrCreate()

data = (
    spark.read.option("escape", '"')
    .option("multiline", "true")
    .csv(str(file_path.absolute()), header=True)
)
data = data.select(["Unnamed: 0", "Name", "Vorname", "Beschrieb"]).withColumnRenamed(
    "Unnamed: 0", "id"
)
data.show()

# Define the extraction
entity = ExtractionEntity(
    name="name",
    instructions="Extract the names of people in the dataset. Do not add any other information",
)

extraction = StructuredExtraction(
    entities=[entity],
)

data_extracted = extraction.fit_extract(
    df=data.filter(col("Beschrieb").isNotNull()),
    llm=llm,
    primary_keys=["id"],
    input_columns=["Beschrieb"],
    batch_size=50,
)

joined = data.join(data_extracted, on="id", how="left")

# Write the data
path_to_write = file_path.parent / "build"

if path_to_write.exists():
    shutil.rmtree(path_to_write)

path_to_write.mkdir(exist_ok=True, parents=True)

joined.write.csv(
    str(path_to_write / "ExtractedData.csv"), header=True, mode="overwrite"
)
