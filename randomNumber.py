# generate random integer values
import random
# specify number of input
inputSize = 1000
# to generate random number list
res = [random.randrange(1, 50, 1) for i in range(inputSize)]

# printing result
print("Random number list is : " + str(res))