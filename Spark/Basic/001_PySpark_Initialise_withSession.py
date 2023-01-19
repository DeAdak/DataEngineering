from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Do not use from code
# Must be passed from spark-submit
master = 'local'
appName = 'PySpark_Initialise'

# config = SparkConf().setAppName(appName).setMaster(master)
# sc = SparkContext(conf=config)

sparkSession = SparkSession.builder.appName('Session_Initialize').getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {sparkSession.sparkContext.appName}')
    print(f'Master: {sparkSession.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

