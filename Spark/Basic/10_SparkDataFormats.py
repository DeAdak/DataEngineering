from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession

master = 'local'
appName = 'PySpark_Data format Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext for SparkSQL
sqlContext = SQLContext(sc)
# You will need to create the SparkSession for streaming
ss = SparkSession(sc)

if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

baseDir = 'file:///tmp/formats/electiondata/'

df = ss.read.csv('file:///home/deba/DBDA_HOME/DataSets/Train_Dataset_withHeader.csv')

df.write.option('header', True).csv(baseDir + "csv")

df.write.mode('overwrite').option('header', False).csv(baseDir + "csv")

df.write.mode('overwrite').parquet(baseDir + 'parquet')

df.write.mode('overwrite').orc(baseDir + 'orc')

df.select('_c0').write.text(baseDir + 'text')

