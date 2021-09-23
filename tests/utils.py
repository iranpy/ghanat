from typing import Union
from pandas.core.frame import DataFrame
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
import pandas as pd


def get_teams():
    d = pd.read_csv('tests/data/team.csv')
    return d


# def _df_to_list(df: SparkDataFrame):
#     lst = df.rdd.toLocalIterator()
#     output = []
#     for item in lst:
#         output.append({col: item[col] for col in df.columns})
#     return output

# def compare_df(first: SparkDataFrame, second: pd.DataFrame):
#     first_lst = _df_to_list(first)
    
#     assert len(first_lst) == len(second), 'row length is not equal'

#     assert len(first.columns) == len(second.columns), 'columns are not equal'

#     columns = first.columns
#     for index, row in second.iterrows():
#         print(index, row)
#         for col in columns:
#             print(col)
#             assert first_lst[index][col] == row[col],\
#                 'dataframes are not equal at row: {} and column: {}'.format(index, col)


def compare_df(first: Union[SparkDataFrame, DataFrame], second: Union[SparkDataFrame, DataFrame]) -> None:
    if type(first) == SparkDataFrame and type(second) == SparkDataFrame:
        # first.except(second)
        pass
    elif type(first) == DataFrame and type(second) == DataFrame:
        diff_df = pd.merge(first, second, how='outer', indicator='Exist')
        diff_df = diff_df.loc[diff_df['Exist'] != 'both']
        assert diff_df.empty, 'pandas dataframes are not equal'
    else:
        raise Exception("Two dataframe must be the same type type")