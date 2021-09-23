# ghanat
Core of ghanat feature store.

### Getting started
This is a simple example with two batch source for input.
```python
from datetime import datetime, timedelta
from ghanat.decorators import feature_runner
from ghanat.source import BatchSource, FileSourceConfig, FileFormatsEnum
from ghanat.tasks import Schedule

start_time = datetime.now() + timedelta(minutes=2)
sc = Schedule('* * * * *', start_time)

class TeamSchema(DataSchema):
    __group_key__ = ['name']
    name = StrField()
    number = IntField()
    role = StrField()

    def aggregate(self, df):
        return df

    def transform(self, df):
        return df

team_source_file = FileSourceConfig('data/team.csv', file_format=FileFormatsEnum.CSV)
team_source = BatchSource(
    spark=spark_session,
    name='team_source',
    description='our team information',
    source_config=team_source_file,
    schema=TeamSchema
)

class RatingSchema(DataSchema):
    __group_key__ = ['number']
    number = IntField()
    soft_skill_rate = IntField()
    tech_skill_rate = IntField()

rating_source_file = FileSourceConfig('data/rating.csv', file_format=FileFormatsEnum.CSV)
rating_source = BatchSource(
    spark=spark_session,
    name='rating_source',
    description='our team rating information',
    source_config=rating_source_file,
    schema=RatingSchema
)

class TeamRatingSchema(DataSchema):
    __group_key__ = ['name']

    number = IntField()
    name = StrField()
    role = StrField()
    soft_skill_rate = IntField()
    tech_skill_rate = IntField()

    entities = ('number',)

@feature_runner(
    source={'team': team_source, 'rating': rating_source},
    online=True,
    offline=True,
    schedule=sc,
    ttl='10d',
    tags=['team'],
    description='our teams rating',
    schema=TeamRatingSchema,
)
def transform_func(team, rating):
    return team.join(rating, team.number == rating.number, 'outer')\
        .select(team.number, team.name, team.role, rating.soft_skill_rate, rating.tech_skill_rate)
```

## Contribution
To contribute in this project, please read our contribution guideline.
