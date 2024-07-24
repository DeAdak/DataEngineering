from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Do not use from code
# Must be passed from spark-submit
master = 'local'
appName = 'PySpark_Initialise'
# ========= STANDALONE ============
# master will be set by terminal command
# start spark master and worker
# $ cd $SPARK_HOME/
# $ sbin/start-master.sh
# $ sbin/start-worker.sh spark://deba-Lenovo:7077
# spark-submit --help
# spark-submit --master spark://deba-Lenovo:7077 /home/deba/DBDA_HOME/DBDA_CODE/SPARK/PYTHON/Core/01_PySpark_Initialise.py
# spark-submit /home/deba/DBDA_HOME/DBDA_CODE/SPARK/PYTHON/Core/001_PySpark_Initialise_withSession.py
# spark-submit --master spark://deba-Lenovo:7077 /home/deba/DBDA_HOME/DBDA_CODE/SPARK/PYTHON/Core/001_PySpark_Initialise_withSession.py

# ========= YARN ============
# vi ~/.bashrc
# export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
# stop master & worker(if running)
# $ sbin/stop-worker.sh
# $ sbin/stop-master.sh
# $HADOOP_HOME/sbin/start-dfs.sh
# $HADOOP_HOME/sbin/start-yarn.sh
# spark-submit --master yarn /home/deba/DBDA_HOME/DBDA_CODE/SPARK/PYTHON/Core/001_PySpark_Initialise_withSession.py
# ===============================

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

