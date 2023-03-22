from pyspark.sql import SparkSession
ss = SparkSession.builder.appName('MySparkStreamingSession')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")\
    .master('local')\
    .getOrCreate()
if ss:
    print(ss.sparkContext.appName)
else:
    print('Could not initialise pyspark session')
# Run the following commands once:
# Start DFS,YARN,SPARK
# /home/deba/DBDA_HOME/hadoop-3.3.4/sbin/start-dfs.sh
# /home/deba/DBDA_HOME/hadoop-3.3.4/sbin/start-yarn.sh
# /home/deba/DBDA_HOME/spark-3.3.1-bin-hadoop3/sbin/start-master.sh
# /home/deba/DBDA_HOME/spark-3.3.1-bin-hadoop3/sbin/start-worker.sh spark://deba-Lenovo:7077
# cd /home/deba/DBDA_HOME/kafka_2.13-3.0.0
# ./bin/zookeeper-server-start.sh config/zookeeper.properties > logs/zk.log 2>&1 &
# ./bin/kafka-server-start.sh config/server.properties > logs/kafka.log 2>&1 &
# ./bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic train_topic --bootstrap-server localhost:9092
# Check if the topic is properly created
# bin/kafka-topics.sh --describe --topic train_topic --bootstrap-server localhost:9092

# Run the following to start the kafka producer, now run this application, after it starts write 'key:value' in kafka producer to see streaming
# bin/kafka-console-producer.sh --topic train_topic --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092

# OR
# Start your program on a different terminal, after it starts write 'key:value' in kafka producer to see streaming
# cd /home/deba/DBDA_HOME/DBDA_CODE/SPARK/PYTHON/Core
# spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 25_SparkStreaming_withKafka.py

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