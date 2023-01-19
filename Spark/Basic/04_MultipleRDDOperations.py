from pyspark import SparkContext, SparkConf, StorageLevel

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
data1 = [1, 2, 3, 4, 5, 6]
data2 = [1, 2, 3, 8, 9, 10]
parallelData1 = sc.parallelize(data1)
parallelData2 = sc.parallelize(data2)
print('Datasets created')
print('=================================')

print('=================================')
print('Fold operation')
print('=================================')
folded = parallelData1.fold(0, lambda x, y: x + y)
print(f'folded value - {folded}')
print('=================================')


print('=================================')
print('Persist Operation')
print('=================================')
parallelData1.persist(StorageLevel.MEMORY_ONLY)
# Set this RDD's storage level to persist its values across operations after the first
# time it is computed. This can only be used to assign a new storage level if the RDD
# does not have a storage level set yet. If no storage level is specified defaults
# to (MEMORY_ONLY)
print('=================================')

print('=================================')
print('Union operation')
print('=================================')
union = parallelData1.union(parallelData2)
union.foreach(print)
print('=================================')


print('=================================')
print('Intersection Operation')
print('=================================')
intersection = parallelData1.intersection(parallelData2)
intersection.foreach(print)
print('=================================')

print('=================================')
print('UnPersist Operation')
print('=================================')
parallelData1.unpersist()
# Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
print('=================================')


