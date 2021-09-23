from pyspark.sql.types import StringType, StructType, StructField, IntegerType

from ghanat.schema import DataSchema, StrField, IntField


class TeamSchema(DataSchema):
    __group_key__ = ['name']

    name = StrField()
    number = IntField()
    role = StrField()


class RatingSchema(DataSchema):
    __group_key__ = ['number']

    number = IntField()
    soft_skill_rate = IntField()
    tech_skill_rate = IntField()


class TeamRatingSchema(DataSchema):
    __group_key__ = ['name']

    number = IntField()
    name = StrField()
    role = StrField()
    soft_skill_rate = IntField()
    tech_skill_rate = IntField()

team_list = [
    ('James', 334, 'PO'),
    ('Michael', 463, 'Designer'),
    ('Maria', 382, 'Back-end'),
]

team_sechema = StructType([
    StructField('name', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('rule', StringType(), True),
])


class HeroSchema(DataSchema):
    __group_key__ = ['name',]

    name = StrField()
    gender = StrField()
    eye_color = StrField()
    race = StrField()
    hair_color = StrField()
    height = IntField()
    publisher = StrField()
    skin_color = StrField()
    alignment = StrField()
    weight = IntField()
