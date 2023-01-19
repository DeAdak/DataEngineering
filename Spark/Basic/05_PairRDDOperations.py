# import findspark

from pyspark import SparkContext, SparkConf

master = 'local'
appName = 'MultipleRDDOperations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print()
print('=================================')
print('Declare an array and parallelize it')
print('=================================')
data1 = [1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6]
data2 = [1, 2, 3, 8, 9, 10]
parallelData1 = sc.parallelize(data1)
parallelData2 = sc.parallelize(data2)

parallelPairRDD1 = parallelData1.map(lambda x: (x, 1))
parallelPairRDD2 = parallelData2.map(lambda x: (x, 2))

parallelPairRDD1.foreach(print)
parallelPairRDD2.foreach(print)

print('=================================')
print('Count by Value')
print('=================================')
countByValue = parallelData1.countByValue()
# Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
print(f'Print : {countByValue}')
print('=================================')

print('=================================')
print('Group By Key')
print('=================================')
groupedByKey = parallelPairRDD1.groupByKey()
# Group the values for each key in the RDD into a single sequence.
# Hash-partitions the resulting RDD with numPartitions partitions.
groupedByKey.foreach(print)
print('=================================')

print('=================================')
print('Reduce By Key')
print('=================================')
reduceByKey = parallelPairRDD1.reduceByKey(lambda x, y: x + y)
# Merge the values for each key using an associative and commutative reduce function.
# This will also perform the merging locally on each mapper before sending results to a
# reducer, similarly to a "combiner" in MapReduce
reduceByKey.foreach(print)
print('=================================')

print('=================================')
print('Aggregate By Key')
print('=================================')
aggregateByKey = parallelPairRDD1.aggregateByKey(0, lambda x, y: x + y, lambda x, y: x + y)
aggregateByKey.foreach(print)
print('=================================')

print('=================================')
print('Sort By Key')
print('=================================')
sortByKey = parallelPairRDD1.sortByKey()
# Sorts this RDD, which is assumed to consist of (key, value) pairs.
sortByKey.foreach(print)
print('=================================')


print('=================================')
print('Count By Key')
print('=================================')
countByKey = parallelPairRDD1.countByKey()
# Count the number of elements for each key, and return the result to the master as a dictionary.
print(countByKey)
print('=================================')

print('=================================')
print('CoGroup Operation')
print('=================================')
coGroup = parallelData1.cogroup(parallelData2)
# For each key k in self or other, return a resulting RDD that contains a tuple with the
# list of values for that key in self as well as other
print(coGroup)
print('=================================')

