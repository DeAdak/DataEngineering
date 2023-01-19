from pyspark import SparkContext, SparkConf

# PySpark is the Python API for Spark.
# Public classes:
# SparkContext: Main entry point for Spark functionality.
# RDD: A Resilient Distributed Dataset(RDD), the basic abstraction in Spark.
# Broadcast: A broadcast variable that gets reused across tasks.
# Accumulator: An "add-only" shared variable that task scan only add values to.
# SparkConf: For configuring Spark.

master = 'local'
appName = 'PySpark_Initialise'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(f'appName: {sc.appName}')
    print(f'master: {sc.master}')
else:
    print('Could not initialise pyspark session')

