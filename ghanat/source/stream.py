from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from ghanat.source.source_configs import SourceConfig
from ghanat.schema.dataschema import DataSchema
from .base import Source


class StreamSource(Source):
    def __init__(
        self,
        spark: SparkSession,
        name: str,
        description: str,
        source_config: SourceConfig,
        schema: DataSchema,
    ) -> None:
        self.name = name
        self.description = description
        self.source_config = source_config
        self.schema = schema

        if self.source_config:
            self.source_config._spark = spark
            self.source_config._schema = schema
    
    def load(self) -> SparkDataFrame:
        df = self.source_config.read()
        # df = self.schema(df)
        return df