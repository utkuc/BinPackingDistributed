import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row
from pyspark.sql.functions import rand, col, array_contains
import json

spark = SparkSession.builder.appName("ddds").master("spark://192.168.0.10:7077").getOrCreate()
bin_capacity = 130
bin_list = []
bin_dic = dict(BinId=0, BinCapacity=bin_capacity, BinUtilization=0, BinItems=[])
bin_list.append(bin_dic)
data_schema = StructType([
    StructField("ValueId", IntegerType(), False),
    StructField("Value", IntegerType(), True),
    StructField("Salt", IntegerType(), True)

])
bin_schema = StructType([
    StructField("BinId", IntegerType(), False),
    StructField("BinUtilization", DoubleType(), False),
    StructField("BinCapacity", IntegerType(), False)

])
data = []
bins = []
# Create 1000 Items with value between (0-100) for packing
data_file = open("data.txt", "w")
for i in range(0, 50000):
    value = (i, random.randint(1, 130), random.randint(1, 100000))
    data.append(value)

with open('data.txt', 'w') as f:
    for item in data:
        f.write("%s,\n" % item[1])

data_df = spark.createDataFrame(data=data, schema=data_schema)
bin_df = spark.createDataFrame(data=bins, schema=bin_schema)

#print(data_df.rdd.glom().collect())
# Reshuffles the dataframe
#data_df = data_df.repartition(2)
#print(data_df.rdd.glom().collect())
#salted_data_df = data_df.withColumn('Salt', col("Salt") + random.randint(1, 10000))
#salted_data_df = salted_data_df.repartition(2)
#print(salted_data_df.rdd.glom().collect())


# To print each nodes data:
# print(data_df.rdd.glom().collect())


# How to sum values for each partition at each node: This is parallel operation
# spark.filter -> BinId = null ise kutuya konmaya ihtiyacı var.
# bin_df.where  ile şu andaki utilizationın altında kalan binlere bak:
# bu binlerden: BinCapacity ve BinUtilization dan şu andaki kapasiteyi çıkar.
# Eğer kapasite şu anda işlenen 'value' için yeterliyse, binid burdaki bine eşlenir.
# Bunların hepsini yaparken yeni RDD ler yaratmak zorundayız, dataframe ve rdd immutable veri yapıları.
def show(index, iterator):
    for row in iterator:
        added_to_bin = False
        last_visited_bin_id = 0
        for dic in bin_list:
            # Value: 15, 130*0.6 = 40 +15 = 55    55<130
            av = dic["BinCapacity"] * dic["BinUtilization"] + row.Value
            if av < dic["BinCapacity"]:
                new_item_list = dic["BinItems"]
                new_item_list.append(dict(ValueId=row.ValueId, Value=row.Value))
                dic["BinItems"] = new_item_list
                added_to_bin = True
                new_utilization = 0
                for item in dic["BinItems"]:
                    new_utilization += item["Value"]
                dic["BinUtilization"] = new_utilization / dic["BinCapacity"]
                # print(dic.items())
                break
            last_visited_bin_id = dic["BinId"]
        if not added_to_bin:
            # Create new bin
            bin_to_create = dict(BinId=last_visited_bin_id + 1, BinCapacity=bin_capacity,
                                 BinUtilization=row.Value / bin_capacity,
                                 BinItems=[dict(ValueId=row.ValueId, Value=row.Value)])
            bin_list.append(bin_to_create)

    yield bin_list


# TODO: Runs for 1 time. We need to determine when its enough.
flag = True
optimization1 = False
utilization_threshold = 0.90
utilization_reduce_multiplier = 0.05
master_list = []
work_df = data_df

while (flag):
    # If there are no bins, create a new bin
    # if len(bin_df.take(1)) == 0:
    #     print("There are no bins, creating new bin...")
    #     new_bin = [[0, 0, bin_capacity]]
    #     new_bin_df = spark.createDataFrame(new_bin)
    #     bin_df = bin_df.union(new_bin_df)
    print("Working Threshold: " + str(utilization_threshold))
    if utilization_threshold < 0:
        flag = False
        break
    bin_list = []
    bin_dic = dict(BinId=0, BinCapacity=bin_capacity, BinUtilization=0, BinItems=[])
    bin_list.append(bin_dic)
    bin_dic_list = work_df.rdd.mapPartitionsWithIndex(show).collect()
    #print("Printing Bins:")
    for result_list in bin_dic_list:
        for bins in result_list:
            #print(bins)
            if bins["BinUtilization"] >= utilization_threshold:
                master_list.append(bins)
    #print("Printing Master Bin:")
    #print(master_list)
    decided_item_value_ids = []
    for bins in master_list:
        for items in bins["BinItems"]:
            decided_item_value_ids.append(items["ValueId"])

    work_df = work_df.filter(work_df.ValueId.isin(decided_item_value_ids) == False)
    if len(work_df.take(1)) == 0:
        break
    utilization_threshold = utilization_threshold - utilization_reduce_multiplier


def functSum(index, iterator):
    sum = 0
    for item in iterator:
        sum += item.Value
    yield str(sum)


print("Result Bins:")
print(master_list)
print("Data's total weight: ")
weights = data_df.rdd.mapPartitionsWithIndex(functSum).collect()
total_weight = int(weights[0]) + int(weights[1])
print("Best possible if weights were unstacked : " + str(total_weight / bin_capacity))
print("Total Number of Bins: " + str(len(master_list)))
# Utilization Perc: %90
# 0.aşama: Verileri workerlara eşit dagıt
# 1.aşama: Her worker Best Fit ile bütün verileri kutulara koy.
# 2.aşama: Workerlar %90 utilization altında olan kutuları parçalar, mastera geri yollar.
# 3.aşama: Shuffle at master.(transport)
# 4.aşama: Repeat 0-3 , gönderilen veriyle gelen veri eşit olana kadar.
# 5.aşama: hala veri varsa, utilizationı düşür. Repeat 0-4 thx
