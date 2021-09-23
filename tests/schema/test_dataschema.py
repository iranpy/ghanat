import pandas as pd

from tests.data.collections import TeamSchema
from tests.utils import get_teams


class TestDataSchema:
    def test_columns_dtype(self):
        team = TeamSchema(get_teams())
        team_df = team.dumps

        assert pd.api.types.infer_dtype(team_df['name']) == 'string', 'name column is not string'
        assert pd.api.types.infer_dtype(team_df['number']) == 'integer', 'number column is not integer'
        assert pd.api.types.infer_dtype(team_df['role']) == 'string', 'role column is not string'

    def test_df_method(self):
        team = TeamSchema(get_teams())
        team_df = team.dumps

        assert type(team.df) == pd.DataFrame