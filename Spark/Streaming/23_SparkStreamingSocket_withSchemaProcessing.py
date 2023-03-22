from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
master = 'local'
appName = 'PySpark_Streaming Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
sc.setSystemProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#You will need to create the sqlContext for SparkSQL
sqlContext = SQLContext(sc)
# You will need to create the SparkSession for streaming
ss = SparkSession(sc)
print(ss.sparkContext.getConf().get("spark.serializer"))
print('Done')
print('=================================')
print('Read traindata over a socket and parse it into a DF')
print('=================================')
# Run this command before starting your program ==> $ nc -lk 9876
trainData = ss.readStream.format('socket') \
    .option('host', 'localhost') \
    .option('port', 9090) \
    .load()
trainData.printSchema()

trainDF = trainData \
    .withColumn('TrainNo', split(trainData.value, '\\,').getItem(0)) \
    .withColumn('TrainIn', split(trainData.value, '\\,').getItem(1)) \
    .withColumn('TrainOut', split(trainData.value, '\\,').getItem(2)) \
    .withColumn('Speed', split(trainData.value, '\\,').getItem(3)) \
    .withColumn('DirIn', split(trainData.value, '\\,').getItem(4)) \
    .withColumn('DirOut', split(trainData.value, '\\,').getItem(5)) \
    .withColumn('Duration', split(trainData.value, '\\,').getItem(6))\
   .drop('value')

trainDF.printSchema()

query = trainDF.writeStream \
    .outputMode('append').format('console').start()

query.awaitTermination()
print('=================================')