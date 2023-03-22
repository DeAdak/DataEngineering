from pyspark import SparkContext, SparkConf, SQLContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#1
# master = 'local'
# appName = 'PySpark_Streaming Operations'
#
# config = SparkConf().setAppName(appName).setMaster(master)
# sc = SparkContext(conf=config)
# sc.setSystemProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
# #You will need to create the sqlContext for SparkSQL
# sqlContext = SQLContext(sc)
# # You will need to create the SparkSession for streaming
# ss = SparkSession(sc)
# print(ss.sparkContext.getConf().get("spark.serializer"))
# print('Done')
# #------------------

#2
ss = SparkSession.builder.appName('MySparkStreamingSession')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")\
    .master('local')\
    .getOrCreate()
#--------------



if ss:
    print(ss.sparkContext.appName)
else:
    print('Could not initialise pyspark session')

#1
# print('=================================')
# print('Read traindata over a socket and parse it into a DF')
# print('=================================')
# # Run this command before starting your program ==> $ nc -lk 9876
# trainData = ss.readStream.format('socket') \
#     .option('host', 'localhost') \
#     .option('port', 9090) \
#     .load()
# trainData.printSchema()
#
# trainDF = trainData \
#     .withColumn('TrainNo', split(trainData.value, '\\,').getItem(0)) \
#     .withColumn('TrainIn', split(trainData.value, '\\,').getItem(1)) \
#     .withColumn('TrainOut', split(trainData.value, '\\,').getItem(2)) \
#     .withColumn('Speed', split(trainData.value, '\\,').getItem(3)) \
#     .withColumn('DirIn', split(trainData.value, '\\,').getItem(4)) \
#     .withColumn('DirOut', split(trainData.value, '\\,').getItem(5)) \
#     .withColumn('Duration', split(trainData.value, '\\,').getItem(6))\
#    .drop('value')
#
# trainDF.printSchema()
#
# query = trainDF.writeStream \
#     .outputMode('append').format('console').start()
#
# query.awaitTermination()
# print('=================================')
#-------------------------------------

#1
# print('=================================')
# print('Read train data over a file location')
# print('=================================')
#
# trainSchema = StructType([StructField('TrainNo', StringType(), True),
#                           StructField('TrainIn', IntegerType(), True),
#                           StructField('TrainOut', IntegerType(), True),
#                           StructField('Speed', IntegerType(), True),
#                           StructField('DirIn', StringType(), True),
#                           StructField('DirOut', StringType(), True),
#                           StructField('Duration', StringType(), True)])
#
# # Please modify the param to CSV with some existing directory
# trainData = ss.readStream.schema(trainSchema) \
#     .option('header', False) \
#     .option('delimiter', ',') \
#     .csv('/tmp/streamingData')
#
# trainData.printSchema()
#
# query = trainData.writeStream \
#     .outputMode('append').format('console').start()
#
# query.awaitTermination()
#
# print('=================================')
#-----------------------------------

# Run the following commands once:
# Start DFS,YARN,SPARK
# bin/zookeeper-server-start.sh config/zookeeper.properties > logs/zk.log 2>&1 &
# bin/kafka-server-start.sh config/server.properties > logs/kafka.log 2>&1 &
# bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic train_topic --bootstrap-server localhost:9092
# Check if the topic is properly created
# bin/kafka-topics.sh --describe --topic train_topic --bootstrap-server localhost:9092

# Run the following to start the kafka producer
# bin/kafka-console-producer.sh --topic train_topic --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092

# Start your program using below code
# spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 26_StreamProcessingWithData.py

#2
print('=================================')
print('Read train dataset over Kafka')
print('=================================')
# Subscribe to 1 topic, with headers
kafkaTrainDf = ss.readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "train_topic")\
  .load()

kafkaTrainDf.printSchema()

kafkaTrainDfData = kafkaTrainDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

kafkaTrainDfData.printSchema()

query = kafkaTrainDfData.writeStream.outputMode('append').format('console').start()

print("Started... ")
query.awaitTermination()

print('=================================')
#--------------------

# Untested code below:
# print('=================================')
# print('Aggregate train data over a windowed timeframe')
# print('=================================')
# trainData = ss.readStream.format('socket') \
#     .option('host', 'localhost') \
#     .option('port', 9876) \
#     .load()
#
# trainData.printSchema()
#
# trainDF = trainData \
#     .withColumn('trainNo', split(trainData.value, '\\|').getItem(0)) \
#     .withColumn('TrainIn', split(trainData.value, '\\|').getItem(1)) \
#     .withColumn('TrainOut', split(trainData.value, '\\|').getItem(2)) \
#     .withColumn('Speed', split(trainData.value, '\\|').getItem(3)) \
#     .withColumn('DirIn', split(trainData.value, '\\|').getItem(4)) \
#     .withColumn('DirOut', split(trainData.value, '\\|').getItem(5)) \
#     .withColumn('Duration', split(trainData.value, '\\|').getItem(6)) \
#     .drop('value')
#
# trainDF = trainDF\
#     .groupby(window(trainDF.trainNo, '10 seconds'))\
#     .agg(count(trainDF.trainNo).alias('TotalTrainsInLast10Seconds'))
#
# query = trainDF.writeStream \
#     .outputMode('append').format('console').start()
#
# query.awaitTermination()
