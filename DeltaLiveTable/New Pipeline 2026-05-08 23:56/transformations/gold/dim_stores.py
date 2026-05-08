import dlt
from pyspark.sql.functions import col

# GOLD Streamig Views on top of silver view (Not Silver Table)
@dlt.view(
    name = "stores_gold_view"
)

def stores_gold_view():
    df = spark.readStream.table("stores_silver_view")
    return df

# CREATE DIM  SCD TYPE =2  TAble (With Auto CDC)
dlt.create_streaming_table(name="dim_stores_scd2", table_properties={"delta.columnMapping.mode": "name"})

dlt.create_auto_cdc_flow(
    target='dim_stores_scd2',
    source='stores_gold_view',
    keys=['store_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)