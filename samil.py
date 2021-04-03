import json
import csv
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

spark = SparkSession.builder.appName("SamilProgramı").master("spark://192.168.0.10:7077").getOrCreate()

df = pd.read_csv('datafile.csv')
textFile = spark.read.text('datafile.csv')

val = textFile.count()
print(val)



#Utilization Perc: %90
#0.aşama: Verileri workerlara eşit dagıt
#1.aşama: Her worker Best Fit ile bütün verileri kutulara koy.
#2.aşama: Workerlar %90 utilization altında olan kutuları parçalar, mastera geri yollar.
#3.aşama: Shuffle at master.(transport)
#4.aşama: Repeat 0-3 , gönderilen veriyle gelen veri eşit olana kadar.
#5.aşama: hala veri varsa, utilizationı düşür. Repeat 0-4 thx