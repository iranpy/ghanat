from ghanat.schema.dataschema import DataSchema
from ghanat.schema.fields import StrField, IntField, MethodField

from tests.utils import get_teams


# class TeamSchemaV2(DataSchema):
#     name = StrField()
#     number = IntField()
#     role = StrField()
#     num_plus = MethodField('do_plus', return_type=IntField())

#     @staticmethod
#     def do_plus(df):
#         return df.number + df.number


class TestFields:
    def test_str_field_load(self):
        f = StrField(label='field_label', required=True)
        result = f.loads('foo')
        assert result == 'foo'
        assert f.label == 'field_label'

    # def test_method_field(self):
    #     team = TeamSchemaV2(get_teams())
    #     team_df = team.dumps
    #     print(team_df)
        # for key, value in team_df.iterrows():
        #     assert value['number'] + value['number'] == value['num_plus']