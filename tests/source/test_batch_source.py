from tests.utils import compare_df, get_teams
from ghanat.source import BatchSource, FileSourceConfig, FileFormatsEnum

from tests.data.collections import TeamSchema


class TestBatchSource:
    def test_read_csv(self, spark_session):
        file_source = FileSourceConfig('tests/data/team.csv', file_format=FileFormatsEnum.CSV)
        source = BatchSource(
            spark=spark_session,
            name='sample_file_source',
            description='sample description',
            source_config=file_source,
            schema=TeamSchema
        )
        df = source.load()

        team = TeamSchema(get_teams())
        team = team.dumps

        compare_df(df.toPandas(), team)