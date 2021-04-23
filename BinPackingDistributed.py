import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import  col
import time

spark = SparkSession.builder.appName("BinPackingDistributed ").master("spark://192.168.0.10:7077").getOrCreate()
bin_capacity = 130
bin_list = []
bin_dic = dict(BinId=0, BinCapacity=bin_capacity, BinUtilization=0, BinItems=[])
bin_list.append(bin_dic)
diagnostic_time = []
collect_diagnostic_time = []
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
# Create 100 Items with value between (0-130) for packing
data_file = open("data.txt", "w")
for i in range(0, 100):
    value = (i, random.randint(1, 130), random.randint(1, 100000))
    data.append(value)

data_df = spark.createDataFrame(data=data, schema=data_schema)
bin_df = spark.createDataFrame(data=bins, schema=bin_schema)


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


flag = True
optimization1 = False
utilization_threshold = 0.90
utilization_reduce_multiplier = 0.05
master_list = []
work_df = data_df
decided_item_value_ids = []
while (flag):
    print("Working on Threshold: " + str(utilization_threshold))
    if utilization_threshold < 0:  # Stop when  utilization is less than 0
        flag = False
        break
    bin_list = []
    bin_dic = dict(BinId=0, BinCapacity=bin_capacity, BinUtilization=0, BinItems=[])
    bin_list.append(bin_dic)
    timer1 = time.perf_counter()
    bin_dic_list = work_df.rdd.mapPartitionsWithIndex(show).collect()  # Call FirstFirst Algorithm for rdd
    timer2 = time.perf_counter()
    # print("Collect took: " + str(timer2-timer1))
    collect_diagnostic_time.append(timer2 - timer1)  # Diagnostic Timer
    #  Create a master_list for bins according to threshold
    for result_list in bin_dic_list:
        for bins in result_list:
            # print(bins)
            if bins["BinUtilization"] >= utilization_threshold:
                master_list.append(bins)
                for items in bins["BinItems"]:  # Keep already added value id's to list so we can filter them out later
                    decided_item_value_ids.append(items["ValueId"])
    timer3 = time.perf_counter()
    # creating a new dataframe for other while loop iteration
    work_df = work_df.filter(work_df.ValueId.isin(decided_item_value_ids) == False)
    timer4 = time.perf_counter()
    # print("Filter took: " + str(timer4-timer3))
    diagnostic_time.append(timer4 - timer3) # Diagnostic Timer
    #  Salting for smooth repartition
    salted_work_df = work_df.withColumn('Salt', col("Salt") + random.randint(1, 10000))
    work_df = salted_work_df.repartition(8)
    if len(work_df.take(1)) == 0:  # If workers doesn't return any new bin, no need to continue for other thresholds.
        break
    utilization_threshold = utilization_threshold - utilization_reduce_multiplier


def functSum(index, iterator):
    sum = 0
    for item in iterator:
        sum += item.Value
    yield str(sum)

# RESULTS OF ALGORITHM

# print("Result Bins:")
# print(master_list)
weights = data_df.rdd.mapPartitionsWithIndex(functSum).collect()
total_weight = 0
for items in weights:
    total_weight += int(items)
total_filter_time = 0
for item in diagnostic_time:
    total_filter_time += item
total_collect_time = 0
for item in collect_diagnostic_time:
    total_collect_time += item


# Printing out results

print("Data's total weight: ")
print(total_weight)
print("Best possible if weights were unstacked : " + str(total_weight / bin_capacity))
print("Total Number of Bins used: " + str(len(master_list)))
print("Total time waited for Filtering: " + str(total_filter_time))
print("Total time waited for Collecting: " + str(total_collect_time))

