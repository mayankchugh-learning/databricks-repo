import dlt
from pyspark.sql.functions import *


# CREATE A STREAMING TABLE

@dlt.table(
    name = 'sales_stg'
)

def sales_stg():
    df = spark.readStream.option("skipChangeCommits", "true")\
        .table('databricksmayank.silver.sales_enr')
    return df

# CREATE A MAT VIEW

@dlt.table(
    name = 'sales_enr'
)

def sales_enr():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("priceAfterDiscount", col('total_amount') - col('discount'))
    return df

    # CREATE A MAT VIEW

@dlt.table(
    name = 'sales_cur'
)

def sales_cur():
    df = spark.readStream.table("sales_enr")
    return df