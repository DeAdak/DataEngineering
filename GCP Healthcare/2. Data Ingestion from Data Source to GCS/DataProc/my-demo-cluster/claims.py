from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when, col, to_date

# Initialize Spark Session
spark = SparkSession.builder.appName('Claims').getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = 'healthcare-bucket-22032025'
CLAIMS_FILE_PATH = f'gs://{GCS_BUCKET}/landing/claims/*.csv'
TEMP_GCS_BUCKET = f'gs://{GCS_BUCKET}/temp/'

#BigQuery Configuration
BQ_PROJECT = 'avd-databricks-demo'
BQ_CLAIMS_TABLE = f'{BQ_PROJECT}.bronze_dataset.claims' # Note: Spark BQ connector prefers NO backticks in the string parameter

# 1. Read raw CSV data 
claims_df = spark.read.option('header', 'true').csv(CLAIMS_FILE_PATH)
 
claims_df = claims_df.withColumn(
    'datasource',
    when(input_file_name().contains('hospital1'), 'HospitalA')
    .when(input_file_name().contains('hospital2'), 'HospitalB')
    .otherwise('None')
)

# 2. Deduplicate records
claims_df = claims_df.dropDuplicates()


# 3. Explicitly cast columns to guarantee they match the BigQuery Table definition
# This prevents write crashes caused by inferSchema guessing wrong types
transformed_df = claims_df.select(
    col("ClaimID").cast("string"),
    col("TransactionID").cast("string"),
    col("PatientID").cast("string"),
    col("EncounterID").cast("string"),
    col("ProviderID").cast("string"),
    col("DeptID").cast("string"),
    to_date(col("ServiceDate"), "yyyy-MM-dd").alias("ServiceDate"),  # Ensures strict BQ DATE format
    to_date(col("ClaimDate"), "yyyy-MM-dd").alias("ClaimDate"),      # Ensures strict BQ DATE format
    col("PayorID").cast("string"),
    col("ClaimAmount").cast("decimal(38,9)").alias("ClaimAmount"),    # Matches BQ NUMERIC precision
    col("PaidAmount").cast("decimal(38,9)").alias("PaidAmount"),      # Matches BQ NUMERIC precision
    col("ClaimStatus").cast("string"),
    col("PayorType").cast("string"),
    col("Deductible").cast("decimal(38,9)").alias("Deductible"),
    col("Coinsurance").cast("decimal(38,9)").alias("Coinsurance"),
    col("Copay").cast("decimal(38,9)").alias("Copay"),
    to_date(col("InsertDate"), "yyyy-MM-dd").alias("InsertDate"),
    to_date(col("ModifiedDate"), "yyyy-MM-dd").alias("ModifiedDate"),
    col("datasource").cast("string")
)

# 4. Save the data securely to BigQuery
Spark guess the data types. When you are writing a DataFrame to BigQuery, Spark already knows what types its columns are.
(claims_df.write.format('bigquery')
				.option('table',BQ_CLAIMS_TABLE)
				.option('temporaryGcsBucket',TEMP_GCS_BUCKET)
				.mode('append')
				.save())

print("Claims data loaded successfully into BigQuery!")