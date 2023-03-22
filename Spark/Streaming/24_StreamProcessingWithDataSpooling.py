from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

if ss:
    print(ss.sparkContext.appName)
else:
    print('Could not initialise pyspark session')


print('=================================')
print('Read train data over a file location')
print('=================================')

trainSchema = StructType([StructField('TrainNo', StringType(), True),
                          StructField('TrainIn', IntegerType(), True),
                          StructField('TrainOut', IntegerType(), True),
                          StructField('Speed', IntegerType(), True),
                          StructField('DirIn', StringType(), True),
                          StructField('DirOut', StringType(), True),
                          StructField('Duration', StringType(), True)])

# Please modify the parameter to CSV with some existing directory
trainData = ss.readStream.schema(trainSchema) \
    .option('header', False) \
    .option('delimiter', ',') \
    .csv('/tmp/streamingData')

trainData.printSchema()

query = trainData.writeStream \
    .outputMode('append').format('console').start()

query.awaitTermination()

print('=================================')