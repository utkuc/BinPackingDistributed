import findspark

findspark.init()

from pyspark import SparkContext

sc = SparkContext()

from random import randint

workers = 4

weights = []

bin_capacity = 100

# ağırlıkları öncesinde gruplayalım
group_size = 5

# ağırlıkları gruplu şekilde rasgele oluşturalım
counter = 0
tempValues = []
for i in range(0, 20):
    counter = counter + 1
    value = randint(0, 100)
    tempValues.append(value)
    if counter == group_size:
        weights.append(tempValues)
        counter = 0
        tempValues = []

RDD_S = sc.parallelize(weights, workers)

RDD_S.collect()

print(weights)


# First fit
def firstFit(blockSize, m, processSize, n):
    result = []
    allocation = [-1] * n

    for i in range(n):
        for j in range(m):
            if blockSize[j] >= processSize[i]:
                allocation[i] = j
                blockSize[j] -= processSize[i]

                break
    for i in range(n):
        if allocation[i] != -1:
            result.append((processSize[i], allocation[i] + 1))
            print(allocation[i] + 1)
        else:
            print("Not Allocated")
    return result


def generateBins(n, c):
    bins = []
    for i in range(n):
        bins.append(c)
    return bins


print(generateBins(5, 10))

# RDD'lere first fit uygulayalım
RDD_FF = RDD_S.map(lambda x: firstFit(generateBins(group_size, bin_capacity), group_size, x, group_size))

RDD_FF.collect()

print("jupyter notebookda RDD_F COLLECTİ GOSTERIYOR PYCHARM GOSTERMIYOR WTF")























