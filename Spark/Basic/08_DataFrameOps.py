from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, when, lit, desc
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StructType

master = 'local'
appName = 'PySpark_Dataframe Operations'
#Use any of these
#------------------------------------------------
# config = SparkConf().setAppName(appName).setMaster(master)
# sc = SparkContext(conf=config)
# # You will need to create the sqlContext
# sqlContext = SQLContext(sc)
#--------------------------------------------------
sparkSession = SparkSession.builder.appName(appName).master(master).getOrCreate()
sc = sparkSession.sparkContext
sqlContext = SQLContext(sc)
#----------------------------------------------------
if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print('====================== Dataframe Operations ================= ')
print()
print('=================================')
print('Load Data into dataframe')
print('=================================')
values = [(1001, 1001), (1020, 1012)]
myRDD = sc.parallelize(values)
myDF = myRDD.toDF()
myDF.show()
print('Data loaded')

print('=================================')
print('Print dataframe default schema')
print('=================================')
myDF.printSchema()
print('=================================')

print('=================================')
print('Create dataframe with simple schema')
print('=================================')
myDF = myRDD.toDF(['TrainNo', 'TrainTime'])
myDF.printSchema()
print('=================================')

print('=================================')
print('Create dataframe with explicit schema')
print('=================================')
TrainNoColumn = StructField('TrainNo', dataType=IntegerType(), nullable=False)
TrainTimeColumn = StructField('TrainTime', dataType=IntegerType(), nullable=False)
schema = StructType([TrainNoColumn, TrainTimeColumn])
myDF = sqlContext.createDataFrame(myRDD, schema=schema)
myDF.printSchema()
print('=================================')

print('=================================')
print('Add a column')
print('=================================')
myDF2 = myDF.withColumn('NewTrainNo', col('TrainNo'))
myDF2.show()
myDF3 = myDF2.withColumn('Country', lit('India'))
myDF3.show()
print('=================================')

print('=================================')
print('Rename a column')
print('=================================')
myDF3 = myDF2.withColumnRenamed('TrainTime', 'NewTrainTime')
myDF3.printSchema()
myDF3.show()
print('=================================')

print('=================================')
print('Drop a column')
print('=================================')
myDF4 = myDF2.drop('TrainTime')
myDF4.printSchema()
myDF4.show()
print('=================================')

print('=================================')
print('Select a column')
print('=================================')
myDF5 = myDF4.select('TrainNo')
myDF5.show()
print('Another approach for selecting column')
myDF5 = myDF4.select(col('TrainNo'))
myDF5.show()
print('=================================')

print('=================================')
print('Filter a column')
print('=================================')
print('Using filter')
myDF4.filter('TrainNo == 1001').show()
print('Using where')
myDF4.where('TrainNo == 1001').show()
print('=================================')

print('=================================')
print('Using When/Otherwise')
print('=================================')
myDF7 = myDF2 \
    .withColumn('TrainNo',
                when(col('TrainNo') == 1001, 'One Thousand One')
                .when(col('TrainNo') == 1020, 'One Thousand Twenty')
                .otherwise('NoTrain'))
myDF7.show()
print('=================================')

print('=================================')
print('Distinct')
print('=================================')
myDF5 = myDF4.distinct()
myDF5.show()
print('Using dropDuplicates column')
myDF5 = myDF4.dropDuplicates(['TrainNo'])
myDF5.show()
print('=================================')

print('=================================')
print('Union and UnionAll')
print('=================================')
data = [(1001, 1010), (1002, 1100), (1003, 1800), (1003, 1800)]
data1 = [(1002, 1010), (1102, 1100), (1003, 1800), (1003, 1800)]
myRDD = sc.parallelize(data)
myDF = myRDD.toDF(['TrainNo', 'TrainTime'])
myDF.show()
myDF.union(myDF).show()
myDF.unionAll(myDF).show()
print('=================================')

print('=================================')
print('Aggregates')
print('=================================')
data = [(1001, 1010), (1002, 1100), (1003, 1800), (1003, 1800)]
myRDD = sc.parallelize(data)
myDF = myRDD.toDF(['TrainNo', 'TrainTime'])
print('====== Base DF ======')
myDF.show()
print('====== Union DF ======')
myDF.union(myDF).show()
print('====== Groupby Avg One column ======')
myDF.union(myDF).groupBy("TrainNo").avg("TrainTime").show()
print('====== Groupby Avg All column ======')
myDF.union(myDF).groupBy("TrainNo").avg().show()
print('====== Groupby count ======')
myDF.union(myDF).groupBy("TrainNo").count().show()
print('====== Groupby Max ======')
myDF.union(myDF).groupBy("TrainNo").max().show()
print('====== Groupby Min ======')
myDF.union(myDF).groupBy("TrainNo").min().show()
print('====== Groupby Sum ======')
myDF.union(myDF).groupBy("TrainNo").sum().show()
print('====== Groupby Pivot ======')
myDF.union(myDF).groupBy().pivot("TrainNo").count().show()

print('=================================')

print('=================================')
print('Sort and orderby a column')
print('=================================')
data = [(1001, 1010), (1002, 1100), (1003, 1800), (1003, 1800)]
myRDD = sc.parallelize(data)
myDF = myRDD.toDF(['TrainNo', 'TrainTime'])
print('====== Base DF ======')
myDF.show()
print('====== Union DF ======')
myDF.union(myDF).show()
print('====== Sort function with a column desc ======')
myDF.union(myDF).sort(col('TrainNo').desc()).show()
print('====== Order by  ======')
myDF.union(myDF).orderBy(col('TrainNo')).show()
print('======  Sort by a column ======')
myDF.union(myDF).sort("TrainNo").show()
print('======  ======')
myDF.union(myDF).orderBy("TrainNo").show()
print('======  ======')
myDF.union(myDF).sort(desc("TrainNo")).show()  # Descending sort order
print('======  ======')
myDF.union(myDF).orderBy(desc("TrainNo")).show()

print('=================================')
