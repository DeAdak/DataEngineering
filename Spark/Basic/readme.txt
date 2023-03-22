01_PySpark_Initialise.py : spark initialization using SparkContext, SparkConf

02_PySpark_Initialise_with_findspark.py : spark initialization using findspark, SparkContext, SparkConf
findspark : if you need to use different versions of spark for the same application. Go to configure (just below the application.py, drop down menu)
Environment Variable--> Name:SPARK_HOME, Value: spark folder

03_SingleRDDOpsWithFileOps.py : SparkContext, SparkConf, findspark, parallelize, foreach, map, flatMap, filter, distinct, sample, count, repartition, getNumPartitions, coalesce, collect
first, takeSample, saveAsTextFile, textFile, max

04_MultipleRDDOperations.py : SparkContext, SparkConf, StorageLevel, fold, union, intersection, persist, unpersist, 

05_PairRDDOperations.py : SparkContext, SparkConf, countByValue, groupByKey, reduceByKey, aggregateByKey, sortByKey,
countByKey, cogroup

06_SqlExamples.py : SparkContext, SparkConf, SQLContext, toDF

07_DataframeOperations.py : parkContext, SparkConf, SQLContext, toDF, schema, printSchema, col, when, withColumn, withColumnRenamed, selectExpr,
sqlContext.read.option, dropDuplicates,

08_DataFrameOps.py : SparkContext, SparkConf, SQLContext, col, when, lit, desc, SparkSession, StructField, IntegerType, StructType
toDF, createDataFrame, drop, select, filter, where, union groupBy avg min count sort desc orderBy

09_SparkDBConnectivity.py : connect spark with mysql and run query

10_SparkDataFormats.py : SparkContext, SparkConf, SQLContext, SparkSession, write.option().csv( __.csv)
df.write.mode('overwrite').parquet( __.parquet) or .orc(.orc)

