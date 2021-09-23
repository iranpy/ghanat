from typing import List
from enum import Enum
from pyspark.sql.types import MapType,StringType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


class SourceConfig:
    def read(self) -> DataFrame:
        pass


class FileFormatsEnum(Enum):
    CSV = 'CSV'
    PARQUET = 'PARQUET'


class FileSourceConfig(SourceConfig):
    """
    source config to read from file source

    Args:
        - path: a str path to input file
        - file_format: a FileFormatsEnum to specify input file format
        - schema_path: path to input file schema
    """
    _spark = None

    def __init__(self, path: str, file_format: FileFormatsEnum, schema_path: str = '') -> None:
        self.path = path
        self.file_format = file_format
        self.schema_path = schema_path
    
    def read(self) -> DataFrame:
        # TODO: get more args for csv and parquet methods and check spark.read('csv')
        if self.file_format == FileFormatsEnum.CSV:
            data = self._spark.read.csv(self.path, header=True)
        elif self.file_format == FileFormatsEnum.PARQUET:
            data = self._spark.read.parquet(self.path)
        return data


class KafkaInputTypeEnum(Enum):
    AVRO = 'AVRO'
    JSON = 'JSON'


class KafkaSourceConfig(SourceConfig):
    """
    source config for connecting to Kafka stream source

    Args:
        - servers: a list of kafka servers: ['server_one:9092', 'server_two:9092',]
        - topics: a list of Kafka topics to subscribe: ['events',]
        - group_id: Kafka consumer group
        - include_headers: bool
    """
    _spark = None
    _schema = None

    def __init__(
        self,
        input_type: KafkaInputTypeEnum,
        servers: List[str],
        topics: List[str],
        group_id: str,
    ) -> None:
        self.input_type = input_type
        self.servers = ','.join(servers)
        self.topics = ','.join(topics)
        self.group_id = group_id

    def read(self) -> DataFrame:
        df = self._spark \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.servers) \
            .option('subscribe', self.topics) \
            .load()

        # TODO: handle avro schema
        if self.input_type == KafkaInputTypeEnum.JSON:
            schema = self._schema.load_fields_schema
            parser = from_json

        # heroes_sechema = StructType([
        #     StructField('name', StringType(), True),
        #     StructField('eye_color', StringType(), True),
        #     StructField('race', StringType(), True),
        #     StructField('hair_color', StringType(), True),
        #     StructField('height', IntegerType(), True),
        #     StructField('publisher', StringType(), True),
        #     StructField('skin_color', StringType(), True),
        #     StructField('alignment', StringType(), True),
        #     StructField('weight', IntegerType(), True),
        # ])
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        # df = df.withColumn("value", parser(df.value, heroes_sechema))
        # print(df.select(df.value))
        # df.show()
        print('=======================', self._schema)
        print('=-==============================', schema)
        df = df.select(parser(df.value, schema))
        print(df)

        # df = df.writeStream.format("console").outputMode("append").start().awaitTermination()
        df.writeStream \
            .format("json") \
            .option("checkpointLocation", "data/") \
            .start("data/hello")
        # return df