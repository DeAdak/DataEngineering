#from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
appName = 'Homework'
master = 'local'
spark=SparkSession.builder.master('local').appName('homework').getOrCreate()
if spark.sparkContext:
    print(f'appName: {spark.sparkContext.appName}')
    print(f'master: {spark.sparkContext.master}')
else:
    print('Not working')

# Load the provided election dataset (Election_Dataset_v_3.0.csv) into Spark and gather the following results:
#RDD=spark.sparkContext.textFile('file:///home/deba/DBDA_HOME/DataSets/Election_Dataset_v_3.0.csv',10)
#RDD.foreach(print())
#     1. Load the data into SparkSQL. Schema as follows
#         a. State
#         b. Year
#         c. ID
#         d. District
#         e. Category
#         f. Candidate_Name
#         g. Gender
#         h. Political_Party_Name
#         i. Political_Party_Code
#         j. Count_1
#         k. Voters
#df=RDD.toDF(['State','Year''ID','District','Category','Candidate_Name','Gender','Political_Party_Name','Political_Party_Code','Count_1','Voters'])
#rdd=spark.sparkContext.parallelize(RDD)
#     2. Load the data using both approaches
#         a. By directly passing the column names as array
#df=RDD.toDF('State','Year','ID','District','Category','Candidate_Name','Gender','Political_Party_Name','Political_Party_Code','Count_1','Voters')
df1 = spark.read.csv("file:///home/deba/DBDA_HOME/DataSets/E1.csv")
columns=('State','Year','ID','District','Category','Candidate_Name','Gender','Political_Party_Name','Political_Party_Code','Count_1','Voters')
df1=df1.toDF(*columns)
df1.printSchema()
df1.show()
print(type(df1))
#         b. By creating a infer schema
df2 = spark.read.option('inferSchema', True).csv("file:///home/deba/DBDA_HOME/DataSets/E1.csv")
df2=df2.toDF(*columns)
df2.printSchema()
#df2.show()
print(type(df2))
#          c. By creating a struct schema
schema1 = StructType()\
    .add('State',StringType(),True)\
    .add('Year',StringType(),True)\
    .add('ID',IntegerType(),True)\
    .add('District',StringType(),True)\
    .add('Category',StringType(),True)\
    .add('Candidate_Name',StringType(),True)\
    .add('Gender',StringType(),True)\
    .add('Political_Party_Name',StringType(),True)\
    .add('Political_Party_Code',StringType(),True)\
    .add('Count_1',IntegerType(),True)\
    .add('Voters',IntegerType(),True)

schema2=StructType([StructField('State',StringType(),True),
                    StructField('Year',StringType(),True),
                    StructField('ID',IntegerType(),True),
                    StructField('District',StringType(),True),
                    StructField('Category',StringType(),True),
                    StructField('Candidate_Name',StringType(),True),
                    StructField('Gender',StringType(),True),
                    StructField('Political_Party_Name',StringType(),True),
                    StructField('Political_Party_Code',StringType(),True),
                    StructField('Count_1',IntegerType(),True),
                    StructField('Voters',IntegerType(),True)])

#DF=spark.read.option('schema',schema1).csv("file:///home/deba/DBDA_HOME/DataSets/Election_Dataset.csv")

DF1=spark.read.schema(schema1).csv("file:///home/deba/DBDA_HOME/DataSets/Election_Dataset.csv")
DF1.show()
print(type(DF1))

DF2=spark.read.schema(schema2).csv("file:///home/deba/DBDA_HOME/DataSets/Election_Dataset.csv")
DF2.show()
print(type(DF2))
DF2.createOrReplaceTempView('election')

#     3. Get the total count of records
print(f'3. Get the total count of records: {DF2.count()}')

sqldf1=spark.sql('select count(*) from election')
sqldf1.show()
#     4. Find the earliest year for which the election data was available
print('4. Find the earliest year for which the election data was available')
print(DF1.select(col('Year')).orderBy(col('Year')).collect()[0])

sqldf2=spark.sql('select Year  from election sort by Year limit 1')
sqldf2.show()
#     5. Find the latest year for which the election data was available
print('5. Find the latest year for which the election data was available')
print(DF1.select(col('Year')).orderBy(col('Year').desc()).collect()[0])

sqldf3=spark.sql('select Year from election sort by Year Desc limit 1')
sqldf3.show()
#     6. Find the count of Male / Female candidate per state
print('6. Find the count of Male / Female candidate per state')
df=DF1.orderBy('State').groupBy('State','Gender').count()
df.show()

sqldf4=spark.sql('select state,Gender,count(Gender) from election group by state,Gender')
sqldf4.show()
#     7. Find how many distinct parties were competing each year
print('7. Find how many distinct parties were competing each year')
df=DF1.groupBy('year','Political_Party_Name').count()
df.show()

sqldf5=spark.sql('select Year,Political_Party_Name,count(Political_Party_Name) from election group by Year,Political_Party_Name')
sqldf5.show()
    #8. Find the total voters for each state per year
print('8. Find the total voters for each state per year')
df=DF1.groupBy('year','state').sum('Voters')
df.show()

sqldf6=spark.sql('select year,state, sum(Voters) from election group by Year,State')
sqldf6.show()

#END