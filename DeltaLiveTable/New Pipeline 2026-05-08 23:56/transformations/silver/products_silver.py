import dlt
from pyspark.sql.functions import *

# STREAMING VIEW
@dlt.view(
    name='products_silver_view',
    comment='Cleaned and validated sales data'
)

def products_silver_view():
    df_products = spark.readStream.table("products_bronze")
    df_products = df_products.withColumn("processDate", current_timestamp())
    return df_products

# SALES SILVER TABLE (WITH UPSERT)
dlt.create_streaming_table(name="products_silver", table_properties={"delta.columnMapping.mode": "name"})

dlt.create_auto_cdc_flow(
    target="products_silver",
    source="products_silver_view",
    keys=["product_id"],
    sequence_by=col("processDate"),
    stored_as_scd_type = 1
)