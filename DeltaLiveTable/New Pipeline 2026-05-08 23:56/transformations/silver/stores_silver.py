import dlt
from pyspark.sql.functions import *

# STREAMING VIEW
@dlt.view(
    name='stores_silver_view',
    comment='Cleaned and validated sales data'
)

def stores_silver_view():
    df_stores = spark.readStream.table("stores_bronze")
    df_stores = df_stores.withColumn("store_name", regexp_replace(col("store_name"),"_",""))
    df_stores = df_stores.withColumn("processDate", current_timestamp())
    return df_stores

# SALES SILVER TABLE (WITH UPSERT)
dlt.create_streaming_table(name="stores_silver", table_properties={"delta.columnMapping.mode": "name"})

dlt.create_auto_cdc_flow(
    target="stores_silver",
    source="stores_silver_view",
    keys=["store_id"],
    sequence_by=col("processDate"),
    stored_as_scd_type = 1
)