from ghanat.source.stream import StreamSource
from ghanat.source import KafkaSourceConfig, KafkaInputTypeEnum

from tests.data.collections import HeroSchema


class TestStreamSource:
    def test_kafka_stream_source(self, spark_session):
        kafka_source = KafkaSourceConfig(
            input_type=KafkaInputTypeEnum.JSON,
            servers=['localhost:9094'],
            topics=['hero'],
            group_id='test',
        )
        source = StreamSource(
            spark=spark_session,
            name='hero_stream_source',
            description='heroes stream',
            source_config=kafka_source,
            schema=HeroSchema,
        )
        df = source.load()
        df.show()