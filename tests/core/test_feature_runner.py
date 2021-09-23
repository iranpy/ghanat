from datetime import datetime, timedelta

from pyspark.sql.session import DataFrame as SparkDataFrame

from ghanat.core.feature_runner import FeatureRunner
from ghanat.source import BatchSource, FileSourceConfig, FileFormatsEnum
from ghanat.tasks import Schedule

from tests.data.collections import TeamSchema, RatingSchema, TeamRatingSchema


class TestFeatureRunner:
    def test_single_input(self, spark_session):
        start_time = datetime.now() + timedelta(minutes=2)
        sc = Schedule('* * * * *', start_time)

        file_source = FileSourceConfig('tests/data/team.csv', file_format=FileFormatsEnum.CSV)
        source = BatchSource(
            spark=spark_session,
            name='sample_file_source',
            description='sample description',
            source_config=file_source,
            schema=TeamSchema
        )

        def transform_team(team):
            return team

        fr = FeatureRunner(
            source={'team': source},
            online=True,
            offline=True,
            schedule=sc,
            ttl='10d',
            tags=['team'],
            description='desc',
            schema=TeamSchema,
        )
        fr.run(transform_team)

        assert type(fr.data) == SparkDataFrame

    def test_multiple_input(self, spark_session):
        start_time = datetime.now() + timedelta(minutes=2)
        sc = Schedule('* * * * *', start_time)

        team_source_file = FileSourceConfig('tests/data/team.csv', file_format=FileFormatsEnum.CSV)
        team_source = BatchSource(
            spark=spark_session,
            name='team_source',
            description='our team information',
            source_config=team_source_file,
            schema=TeamSchema
        )

        rating_source_file = FileSourceConfig('tests/data/rating.csv', file_format=FileFormatsEnum.CSV)
        rating_source = BatchSource(
            spark=spark_session,
            name='rating_source',
            description='our team rating information',
            source_config=rating_source_file,
            schema=RatingSchema
        )

        def transform_func(team, rating):
            # TODO: select should be run in parent class background and pass to schema
            return team.join(rating, team.number == rating.number, 'outer')\
                .select(team.number, team.name, team.role, rating.soft_skill_rate, rating.tech_skill_rate)

        fr = FeatureRunner(
            source={'team': team_source, 'rating': rating_source},
            online=True,
            offline=True,
            schedule=sc,
            ttl='10d',
            tags=['team'],
            description='desc',
            schema=TeamRatingSchema,
        )
        fr.run(transform_func)
        fr.data.show()
        assert type(fr.data) == SparkDataFrame