import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_date, date_sub, to_date, add_months, year, month, date_format
from pyspark.sql import functions as F
from pyspark.sql.window import Window

credentials_location = '/home/terryz/.gc/my-creds.json'

conf = SparkConf() \
    .setAppName('test') \
    .set("spark.jars", "gs://animated-scope-447904-d6-ecopulse-bucket/jars/gcs-connector-hadoop3-latest.jar,gs://animated-scope-447904-d6-ecopulse-bucket/jars/spark-bigquery-with-dependencies_2.12-0.24.0.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

sc = SparkContext(conf=conf)
hadoop_conf = sc._jsc.hadoopConfiguration()

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

#data load
sp500 = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/SP500_data.csv", header=True, inferSchema=True)
dgs10 = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/DGS10_data.csv", header=True, inferSchema=True)
vixcls = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/VIXCLS_data.csv", header=True, inferSchema=True)
effr = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/EFFR_data.csv", header=True, inferSchema=True)
cpiaucsl = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/CPIAUCSL_data.csv", header=True, inferSchema=True)
pcepi = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/PCEPI_data.csv", header=True, inferSchema=True)
civpart = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/CIVPART_data.csv", header=True, inferSchema=True)
indpro = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/INDPRO_data.csv", header=True, inferSchema=True)
csushpisa = spark.read.csv("gs://animated-scope-447904-d6-ecopulse-bucket/CSUSHPISA_data.csv", header=True, inferSchema=True)

# Get the date 10 years ago
ten_years_ago = add_months(current_date(), -12 * 10)

# Filter each dataset to the last 10 years
sp500_filtered = sp500.filter(to_date(sp500['date'], 'yyyy-MM-dd') >= ten_years_ago)
dgs10_filtered = dgs10.filter(to_date(dgs10['date'], 'yyyy-MM-dd') >= ten_years_ago)
vixcls_filtered = vixcls.filter(to_date(vixcls['date'], 'yyyy-MM-dd') >= ten_years_ago)
effr_filtered = effr.filter(to_date(effr['date'], 'yyyy-MM-dd') >= ten_years_ago)

# Perform full outer joins to include all dates, with nulls for missing data
sp500_dgs10_vixcls_effr_10years_daily = sp500_filtered \
    .join(dgs10_filtered, on='date', how='outer') \
    .join(vixcls_filtered, on='date', how='outer') \
    .join(effr_filtered, on='date', how='outer')

# add month and year
sp500_dgs10_vixcls_effr_10years_daily = sp500_dgs10_vixcls_effr_10years_daily \
    .withColumn("month", date_format("date", "MM")) \
    .withColumn("year", date_format("date", "yyyy")) \
    .withColumn("year_month", date_format("date", "yyyy-MM"))

# add categorical data for sp500
sp500_dgs10_vixcls_effr_10years_daily = sp500_dgs10_vixcls_effr_10years_daily.withColumn(
    'SP500_daily_change_category',
    F.when(F.col('SP500') > F.lag('SP500', 1).over(Window.orderBy('date')) * 1.01, 'Increase') \
     .when(F.col('SP500') < F.lag('SP500', 1).over(Window.orderBy('date')) * 0.99, 'Decrease') \
     .otherwise('Constant')
)

# Similarly transform the monthly series
cpiaucsl_filtered = cpiaucsl.filter(to_date(cpiaucsl['date'], 'yyyy-MM-dd') >= ten_years_ago)
pcepi_filtered = pcepi.filter(to_date(pcepi['date'], 'yyyy-MM-dd') >= ten_years_ago)
civpart_filtered = civpart.filter(to_date(civpart['date'], 'yyyy-MM-dd') >= ten_years_ago)
indpro_filtered = indpro.filter(to_date(indpro['date'], 'yyyy-MM-dd') >= ten_years_ago)
csushpisa_filtered = csushpisa.filter(to_date(csushpisa['date'], 'yyyy-MM-dd') >= ten_years_ago)

cpiaucsl_pcepi_civpart_indpro_csushpisa_10years_monthly = cpiaucsl_filtered \
    .join(pcepi_filtered, on='date', how='outer') \
    .join(civpart_filtered, on='date', how='outer') \
    .join(indpro_filtered, on='date', how='outer') \
    .join(csushpisa_filtered, on='date', how='outer')

cpiaucsl_pcepi_civpart_indpro_csushpisa_10years_monthly = cpiaucsl_pcepi_civpart_indpro_csushpisa_10years_monthly \
    .withColumn("month", date_format("date", "MM")) \
    .withColumn("year", date_format("date", "yyyy")) \
    .withColumn("year_month", date_format("date", "yyyy-MM"))

# Perform left join by 'year_month' column to create the merged table
EcoPulse_merged = sp500_dgs10_vixcls_effr_10years_daily \
    .join(cpiaucsl_pcepi_civpart_indpro_csushpisa_10years_monthly.drop('date', 'year', 'month'), on='year_month', how='left')

project_id = 'animated-scope-447904-d6'
dataset_id = 'ecopulse_bq_dw'
daily_table_id = 'sp500_dgs10_vixcls_effr_10years_daily' 
monthly_table_id = 'cpiaucsl_pcepi_civpart_indpro_csushpisa_10years_monthly'
merged_table_id = 'ecopulse_merged'

# Write the DataFrames to BigQuery
sp500_dgs10_vixcls_effr_10years_daily \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'animated-scope-447904-d6-ecopulse-bucket') \
    .option('table', f'{project_id}:{dataset_id}.{daily_table_id}') \
    .mode("overwrite") \
    .save()

cpiaucsl_pcepi_civpart_indpro_csushpisa_10years_monthly \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'animated-scope-447904-d6-ecopulse-bucket') \
    .option('table', f'{project_id}:{dataset_id}.{monthly_table_id}') \
    .mode("overwrite") \
    .save()

EcoPulse_merged \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'animated-scope-447904-d6-ecopulse-bucket') \
    .option('table', f'{project_id}:{dataset_id}.{merged_table_id}') \
    .mode("overwrite") \
    .save()