import dlt
from pyspark.sql.functions import *

# CREATE STREAMING TABLE [USING AUTOLOADER]
@dlt.table(
    name = 'autoval_table'
)

def autoval_table():
  df = spark.readStream.format("cloudFiles")\
      .option("cloudFiles.format", "csv")\
          .load("/Volumes/databricksmayank/bronze/autovol/raw/")
  return df


# CREATE STREAMING TABLE 
@dlt.table(
    name = 'autoval_table_enr'
)

def autoval_table_enr():
  df = spark.read.table('autoval_table')
  df = df.withColumn('flag', lit("Yes"))
  return df