from pyspark import SparkContext, SparkConf, SQLContext
# The entry point for working with structured data (rows and columns) in Spark, in Spark 1.x.
# As of Spark 2.0, this is replaced by SparkSession. However, we are keeping the class here for backward compatibility.
# A SQLContext can be used create DataFrame, register DataFrame as tables,
# execute SQL over tables, cache tables, and read parquet files.

master = 'local'
appName = 'MultipleRDDOperations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

sqlContext = SQLContext(sc)

data1 = [(1, 1,3), (2, 2,3), (3, 3,3), (4, 4,3), (5, 5,3), (6, 6,3)]
rdd = sc.parallelize(data1)
print(type(rdd))
print(rdd)
df1 = rdd.toDF()
print(type(df1))
print(df1)
print(df1.schema)
df1.printSchema()

df2 = rdd.toDF(["TrainNo", "TrainSpeed","TrainID"])
print(df2)
df2.printSchema()

parallelData1 = sc.parallelize(data1)
print('Datasets created')

print('=================================')
print('Create a DF without schema')
print('=================================')
df = parallelData1.toDF()
df.show()
print('=================================')


