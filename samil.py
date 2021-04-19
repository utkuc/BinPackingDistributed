import json
import csv
import os
import random

import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.appName("ddds").master("spark://192.168.0.10:7077").getOrCreate()
columns = ["valueid", "value", "currentbinid"]
data = []

#Create Items with value between (0-100) for packing
for i in range(0, 1000):
    value = (i, random.randint(0, 100), 0)
    data.append(value)
rdd = spark.sparkContext.parallelize(data,4)
dfFromRDD1 = rdd.toDF(columns)

def testFunction(row):
    valueid=row.valueid
    value=100+row.value
    currentbinid = row.currentbinid
    return valueid, value, currentbinid

rdd2 = dfFromRDD1.rdd.map(lambda x: testFunction(x))
print(rdd2.take(100))



dfFromRDD1.show(truncate=False)


# Utilization Perc: %90
# 0.aşama: Verileri workerlara eşit dagıt
# 1.aşama: Her worker Best Fit ile bütün verileri kutulara koy.
# 2.aşama: Workerlar %90 utilization altında olan kutuları parçalar, mastera geri yollar.
# 3.aşama: Shuffle at master.(transport)
# 4.aşama: Repeat 0-3 , gönderilen veriyle gelen veri eşit olana kadar.
# 5.aşama: hala veri varsa, utilizationı düşür. Repeat 0-4 thx
