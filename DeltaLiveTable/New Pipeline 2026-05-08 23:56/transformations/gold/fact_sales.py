import dlt
from pyspark.sql.functions import col

# GOLD Streamig Views on top of silver view (Not Silver Table)
@dlt.view(
    name = "sales_gold_view"
)

def sales_gold_view():
    df = spark.readStream.table("sales_silver_view")
    return df

# CREATE FACT TAble (With Auto CDC)
dlt.create_streaming_table(name="fact_sales", table_properties={"delta.columnMapping.mode": "name"})

dlt.create_auto_cdc_flow(
    target='fact_sales',
    source='sales_gold_view',
    keys=['sales_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=1
)