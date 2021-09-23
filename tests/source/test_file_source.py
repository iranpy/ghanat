from ghanat.source import FileSourceConfig, FileFormatsEnum

from tests.utils import get_teams
from tests.utils import compare_df
from tests.data.collections import team_sechema, TeamSchema


class TestFileSource:
    def test_read_csv(self, spark_session):
        file_source = FileSourceConfig('tests/data/team.csv', file_format=FileFormatsEnum.CSV)
        file_source._spark = spark_session
        source_data = file_source.read()
        team = TeamSchema(source_data)
        team = team.dumps

        sec_team = TeamSchema(get_teams())
        sec_team = sec_team.dumps

        compare_df(team.toPandas(), sec_team)