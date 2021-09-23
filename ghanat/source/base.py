from pyspark.sql.dataframe import DataFrame as SparkDataFrame


class Source:
    def load(self) -> SparkDataFrame:
        pass

    # def transform(self, input_data: SparkDataFrame) -> DataSchema:
    #     pass