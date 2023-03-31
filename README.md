# DataEngineering
My hands on journey to become a Data Enginer

Project 1: Load data to your POSTGRESA DATABASE using Python (ipynb).

Project 2: Load data to your POSTGRESA DATABASE, create DATAWAREHOUSE (FACTtable,DIMENSIONtable), run QUERY for same outout and check the time.

## HIVE
  Project 1 : MANAGED/EXTERNAL/PARTITIONED Table Creation, Loading DATA, run Query & compare result.  

## SPARK
  Basic
  
Spark initialization using SparkContext, SparkConf
Spark initialization using findspark, SparkContext, SparkConf
findspark : if you need to use different versions of spark for the same application. Go to configure (just below the application.py, drop down menu)
Environment Variable--> Name:SPARK_HOME, Value: spark folder
SparkContext, SparkConf, findspark, parallelize, foreach, map, flatMap, filter, distinct, sample, count, repartition, getNumPartitions, coalesce, collect
first, takeSample, saveAsTextFile, textFile, max, StorageLevel, fold, union, intersection, persist, unpersist, countByValue, groupByKey, reduceByKey, aggregateByKey, sortByKey,
countByKey, cogroup, SQLContext, toDF, schema, printSchema, col, when, withColumn, withColumnRenamed, selectExpr,
sqlContext.read.option, dropDuplicates, col, when, lit, desc, SparkSession, StructField, IntegerType, StructType
toDF, createDataFrame, drop, select, filter, where, union groupBy avg min count sort desc orderBy
Connect spark with mysql and run query

	Streaming
Realtime data streaming using socket
Realtime data streaming and manipulation using socket
Realtime data streaming and structuring using socket
Realtime data streaming and structuring using DataSpool
Realtime data streaming using Kafka

## Cassandra: Basics and Assignment

##kafkaCassandraPY: Get input from user using Kafka, store it to Cassandra DB, do a sentiment analysis.