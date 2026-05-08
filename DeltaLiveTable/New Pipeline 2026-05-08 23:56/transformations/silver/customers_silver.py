import dlt
from pyspark.sql.functions import *

# STREAMING VIEW
@dlt.view(
    name='customers_silver_view',
    comment='Cleaned and validated sales data'
)

def customers_silver_view():
    df_customers = spark.readStream.table("customers_bronze")
    df_customers = df_customers.withColumn("name", upper(col("name")))
    df_customers = df_customers.withColumn("domain", split(col("email"),"@")[1])
    df_customers = df_customers.withColumn("processDate", current_timestamp())
    return df_customers

# SALES SILVER TABLE (WITH UPSERT)
dlt.create_streaming_table(name="customers_silver", table_properties={"delta.columnMapping.mode": "name"})

dlt.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_silver_view",
    keys=["customer_id"],
    sequence_by=col("processDate"),
    stored_as_scd_type = 1
)