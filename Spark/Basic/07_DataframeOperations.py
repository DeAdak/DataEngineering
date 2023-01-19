from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, when
# Important classes of Spark SQL and DataFrames
# pyspark.sql.functions List of built-in functions available for DataFrame
# Returns a ~pyspark.sql.Column based on the given column name.


master = 'local'
appName = 'PySpark_Dataframe Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext
sqlContext = SQLContext(sc)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print('====================== Dataframe Operations ================= ')
print()
print('=================================')
print('Load Data into dataframe')
print('=================================')
data1 = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]
parallelData1 = sc.parallelize(data1)
print('Datasets created')

print('=================================')
print('Create a DF without schema')
print('=================================')
df = parallelData1.toDF()
df.show()
print('=================================')

print()
print('=================================')
print('Create a DF with schema directly as an argument')
print('=================================')
df = parallelData1.toDF(['col1', 'col2'])
df.show()
print('=================================')

print()
print('=================================')
print('Create a DF with schema with array argument')
print('=================================')
colNameArray = ['col1', 'col2']
df = parallelData1.toDF(colNameArray)
df.show()
print('=================================')

print()
print('=================================')
print('Add a new column')
print('=================================')
# Remember to import "from pyspark.sql.functions import col"
df = df.withColumn('newCol', col('col1'))
# Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
df.show()
print('=================================')

print()
print('=================================')
print('Rename a new column')
print('=================================')
# Remember to import "from pyspark.sql.functions import col"
# and from pyspark.sql import functions as F
# df = df.withColumnRenamed(F.col("col2"), F.col("newColUpdated"))
df = df.withColumnRenamed('col2', 'newColUpdated')
# Returns a new DataFrame by renaming an existing column. This is a no-op if schema
# doesn't contain the given column name.
df.show()
print('=================================')

print()
print('=================================')
print('Drop a new column')
print('=================================')
# Remember to import "from pyspark.sql_code.functions import col"
df = df.drop('newColUpdated')
df.show()
print('=================================')

print()
print('=================================')
print('Select column')
print('=================================')
df = df.select('col1')
df.show()
print('=================================')

print()
print('=================================')
print('Select column')
print('=================================')
df = df.selectExpr('col1 as someNewColumn')
df.show()
print('=================================')

print()
print('=================================')
print('Read from a file without header')
print('=================================')
df = sqlContext.read.option('header', False).option('delimiter', ',').csv(
    'file:///home/deba/DBDA_HOME/DataSets/Train_Dataset_withoutHeader.csv')
df.show()
df.select('_c0').show()
print('=================================')

print()
print('=================================')
print('Read from a file with header')
print('=================================')
df = sqlContext.read.option('header', True) \
    .option('delimiter', '|') \
    .csv('file:///home/deba/DBDA_HOME/DataSets/Train_Dataset_withHeader.csv')
df.show()
df.printSchema()
df.select('TrainNo').show()
print('=================================')

print()
print('=================================')
print('Filter data using filter')
print('=================================')
df.filter('TrainNo == 1001').show()
# Filters rows using the given condition.
# where is an alias for filter.
print('=================================')

print()
print('=================================')
print('Filter data using where')
print('=================================')
df.where('TrainNo == 1001').show()
print('=================================')

print()
print('=================================')
print('Using with and when')
print('=================================')
# Remember to import 'from pyspark.sql_code.functions import when'
df.withColumn('TrainNo', when(col('TrainNo') == 1001, 'OneThousandOne')
              .when(col('TrainNo') == 1010, 'OneThousandTen')
              .otherwise('No Train')).show()
print('=================================')

print()
print('=================================')
print('Using distinct')
print('=================================')
# Remember to import 'from pyspark.sql.functions import when'
df.distinct().show()
print('=================================')

print()
print('=================================')
print('Using dropDuplicates')
print('=================================')
# Remember, drop duplicates in python takes an array
df.dropDuplicates(['DirIn']).show()
# Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.
print('=================================')


# Untested code : DO NOT USE
# print()
# print('=================================')
# print('Using Group By Operations')
# print('=================================')
# print(f'Count : {df.count()}')
# df.show()
# print('========')
# df.groupBy('TrainNo').avg().show()
# print('========')
# df.groupBy(['TrainNo']).count().show()
# print('========')
# df.groupBy(['TrainNo']).max().show()
# print('========')
# df.groupBy(['TrainNo']).min().show()
# print('========')
# df.groupBy(['TrainNo']).sum().show()
# print('========')
# df.groupBy().pivot(['TrainNo']).count().show()
print('=================================')
