import findspark
from pyspark import SparkContext, SparkConf

# Ensure your SPARK_HOME is already initialised

# Find spark home, and initialize by adding pyspark to sys.path.
# If SPARK_HOME is defined, it will be used to put pyspark on sys.path.
# Otherwise, common locations for spark will be searched.

master = 'local'
appName = 'PySpark_Initialise_with_findspark'

# adds pyspark to sys.path at runtime
findspark.init()

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

