from pyspark import SparkContext, SparkConf

master = 'local'
appName = 'SingleRDDOperations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print('======================== Transformations ================================')

print()
print('=================================')
print('Declare an array and parallelize it')
print('=================================')
data = [1, 2, 3]
print(data)
parallelData = sc.parallelize(data)
# Distribute a local Python collection to form an RDD.
print(type(parallelData))
print(parallelData)
parallelData.foreach(print)
# Applies a function to all elements of this RDD.
print('=================================')

print()
print('=================================')
print('Process RDD using map')
print('=================================')
squares = parallelData.map(lambda x: x * x)
# Return a new RDD by applying a function to each element of this RDD
print(type(squares))
print(squares)
squares.foreach(print)
print('=================================')

print()
print('=================================')
print('Process RDD using flatmap')
print('=================================')
explode = parallelData.flatMap(lambda x: range(x))
# Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
print(explode)
explode.foreach(print)
print('=================================')

print()
print('=================================')
print('Filter an RDD')
print('=================================')
filtered = explode.filter(lambda x: x != 0)
# Return a new RDD containing only the elements that satisfy a predicate.
print(filtered)
filtered.foreach(print)
print('=================================')

print()
print('=================================')
print('Distinct over RDD')
print('=================================')
distinct = filtered.distinct()
# Return a new RDD containing the distinct elements in this RDD.
print(distinct)
distinct.foreach(print)
print('=================================')
print()
print('=================================')
print('Sample over RDD')
print('=================================')
largeRangeData = sc.parallelize(range(1000))
print(largeRangeData)
sample = largeRangeData.sample(False, 0.01)
# Return a sampled subset of this RDD. This is not guaranteed to provide exactly the
# fraction specified of the total count of the given DataFrame.
print(sample)
sample.foreach(print)
print('==========')
print(sample.count())
# Return the number of elements in this RDD
print('==========')
print('=================================')
sample1 = largeRangeData.sample(False, 0.01)
sample1.foreach(print)
print('==========')
print(sample1.count())
print('=================================')
sample2 = largeRangeData.sample(False, 0.01)
sample2.foreach(print)
print('==========')
print(sample2.count())
print('=================================')
sample3 = largeRangeData.sample(False, 0.01)
sample3.foreach(print)
print('==========')
print(sample3.count())
print('=================================')
sample4 = largeRangeData.sample(False, 0.01)
sample4.foreach(print)
print('==========')
print(sample4.count())

print()
print('=================================')
print('Repartition RDD')
print('=================================')
repartitionedData = largeRangeData.repartition(10)
# Return a new RDD that has exactly numPartitions partitions.
# Can increase or decrease the level of parallelism in this RDD. Internally,
# this uses a shuffle to redistribute data. If you are decreasing the number of
# partitions in this RDD, consider using coalesce, which can avoid performing a shuffle.
print(repartitionedData)
print(f'Data was repartitioned in {repartitionedData.getNumPartitions()} partitions')
# Returns the number of partitions in RDD
print('=================================')

print()
print('=================================')
print('Coalesce RDD, to decrease the number of partitions')
print('=================================')
coalescedData = repartitionedData.coalesce(2)
# Return a new RDD that is reduced into numPartitions partitions.
print(coalescedData)
print(f'Data was repartitioned in {coalescedData.getNumPartitions()} partitions')
print('=================================')

print('~~~~~~~~~~~~~~~~~~~~~~~~~~ Actions ~~~~~~~~~~~~~~~~~~~~~~~~~~')

print()
print('=================================')
print('Collect the data onto driver')
print('=================================')
simpleData = largeRangeData.collect()
# Return a list that contains all of the elements in this RDD.
print(type(simpleData))
print(simpleData)
print('=================================')

print()
print('=================================')
print('First')
print('=================================')
firstElem = largeRangeData.first()
# Return the first element in this RDD.
print(f'First elem : {firstElem}')
print('=================================')

print()
print('=================================')
print('Retrieve the count of RDD')
print('=================================')
count = largeRangeData.count()
print(f'Count = {count}')
print('=================================')

print()
print('=================================')
print('Take N elements')
print('=================================')
sample = largeRangeData.take(5)
# Take the first num elements of the RDD.
# It works by first scanning one partition, and use the results from that partition
# to estimate the number of additional partitions needed to satisfy the limit.
# This method should only be used if the resulting array is expected to be small,
# as all the data is loaded into the driver's memory.

print(sample)
print(f'')
print('=================================')

print()
print('=================================')
print('Take N random elements')
print('=================================')
sample = largeRangeData.takeSample(True, 10)
# Return a fixed-size sampled subset of this RDD.
# This method should only be used if the resulting array is expected to be small,
# as all the data is loaded into the driver's memory
print(f'datatype of sample : {type(sample)}')
print(f'Sample = {sample}')
print('=================================')


print()
print('=================================')
print('Save as text file')
print('=================================')
largeRangeData.saveAsTextFile('file:///tmp/file_from_spark3.txt')
# Save this RDD as a text file, using string representations of elements
print(f'File saved successfully')
print('=================================')


print()
print('=================================')
print('Load a text file')
print('=================================')
trainData = sc.textFile('file:///home/deba/DBDA_HOME/DataSets/Train_Dataset_withHeader.csv', 10)
# Read a text file from HDFS, a local file system (available on all nodes), or any
# Hadoop-supported file system URI, and return it as an RDD of Strings.
# The text files must be encoded as UTF-8.
print(f'File read successfully into {trainData.getNumPartitions()} partitions')
#RDD=sc.parallelize(trainData)
trainData.foreach(print)
print('=================================')

print()
print('=================================')
print('Find the max of speed from traindata (without using dataframes) ')
print('=================================')
maxSpeed = trainData.map(lambda x: x.split('|')).map(lambda x: x[3])\
    .filter(lambda x: x != 'Speed').max()

maxSpeed1 = maxSpeed.map(lambda x: x*x)
maxSpeed2 = maxSpeed1.filter(lambda x: x != 100)

maxSpeedValue = maxSpeed2.max()


print(maxSpeed)
maxSpeed.foreach(print)
print('=================================')
