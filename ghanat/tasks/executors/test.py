# from pyspark.sql import SparkSession


# spark = SparkSession.builder.appName("appName").getOrCreate()
# sc = spark.sparkContext
# rdd = sc.parallelize([1,2,3,4,5,6,7])

# print(rdd.count())
from ghanat.tasks.executors.test1 import ALL

# class B:
#     def s(self):
#         return 100

class A(ALL):
    def b(self):
        return 500

import cloudpickle
picklefile = open('/home/ali/Projects/objs1.pkl', 'wb')
cloudpickle.dump(A, picklefile)