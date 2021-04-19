import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from pyspark.sql.functions import rand

spark = SparkSession.builder.appName("ddds").master("spark://172.20.64.1:7077").getOrCreate()
data_schema = StructType([
    StructField("ValueId", IntegerType(), False),
    StructField("Value", IntegerType(), True),
    StructField("BinId", IntegerType(), True)

])
bin_schema = StructType([
    StructField("BinId", IntegerType(), False),
    StructField("BinUtilization", IntegerType(), False),
    StructField("BinCapacity", IntegerType(), False)

])
bin_capacity = 130
data = []
bins = []
# Create 1000 Items with value between (0-100) for packing
for i in range(0, 10):
    value = (i, random.randint(0, 100), None)
    data.append(value)
data_df = spark.createDataFrame(data=data, schema=data_schema)
bin_df = spark.createDataFrame(data=bins, schema=bin_schema)

print(data_df.rdd.glom().collect())
# Reshuffles the dataframe
data_df = data_df.repartition(2)
print(data_df.rdd.glom().collect())


# To print each nodes data:
# print(data_df.rdd.glom().collect())
flag = True

# How to sum values for each partition at each node: This is parallel operation
# spark.filter -> BinId = null ise kutuya konmaya ihtiyacı var.
# bin_df.where  ile şu andaki utilizationın altında kalan binlere bak:
# bu binlerden: BinCapacity ve BinUtilization dan şu andaki kapasiteyi çıkar.
# Eğer kapasite şu anda işlenen 'value' için yeterliyse, binid burdaki bine eşlenir.
# Bunların hepsini yaparken yeni RDD ler yaratmak zorundayız, dataframe ve rdd immutable veri yapıları.
def show(index, iterator):
    sum = 0
    for row in iterator:
        # Burda print Sum dersen: pycharm print atmaz ama workerlar kendi outputlarında gösterir, örnek:
        # print(row.Value) bunu sadece workerin kendi outputunda görebilirsin. Mastera collect atmadıgın sürece
        # aşağıda yield dan sonra collect atılcak, yani sumlar workerlarda hesaplanıp sadece sum sonuçları masterda birleşcek/gösterilcek.
        sum += row.Value
    yield 'Partition Index: ' + str(index) + " Sum of data values in partition: " + str(sum)


# TODO: Runs for 1 time. We need to determine when its enough.
while (flag):
    # If there are no bins, create a new bin
    if len(bin_df.take(1)) == 0:
        print("There are no bins, creating new bin...")
        new_bin = [[0, 0, bin_capacity]]
        new_bin_df = spark.createDataFrame(new_bin)
        bin_df = bin_df.union(new_bin_df)

    print(data_df.rdd.mapPartitionsWithIndex(show).collect())

    flag = False
bin_df.show(truncate=False)
#
# def testFunction(row):
#     valueid=row.valueid
#     value=100+row.value
#     currentbinid = row.currentbinid
#     return valueid, value, currentbinid
#
# rdd2 = dfFromRDD1.rdd.map(lambda x: testFunction(x))
# print(rdd2.take(100))
#
#
#
# dfFromRDD1.show(truncate=False)


# Utilization Perc: %90
# 0.aşama: Verileri workerlara eşit dagıt
# 1.aşama: Her worker Best Fit ile bütün verileri kutulara koy.
# 2.aşama: Workerlar %90 utilization altında olan kutuları parçalar, mastera geri yollar.
# 3.aşama: Shuffle at master.(transport)
# 4.aşama: Repeat 0-3 , gönderilen veriyle gelen veri eşit olana kadar.
# 5.aşama: hala veri varsa, utilizationı düşür. Repeat 0-4 thx
