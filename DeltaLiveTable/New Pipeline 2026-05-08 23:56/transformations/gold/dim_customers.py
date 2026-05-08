import dlt
from pyspark.sql.functions import col

# GOLD Streamig Views on top of silver view (Not Silver Table)
@dlt.view(
    name = "customers_gold_view"
)

def customers_gold_view():
    df = spark.readStream.table("customers_silver_view")
    return df

# CREATE DIM  SCD TYPE =2  TAble (With Auto CDC)
dlt.create_streaming_table(name="dim_customers_scd2", table_properties={"delta.columnMapping.mode": "name"})

dlt.create_auto_cdc_flow(
    target='dim_customers_scd2',
    source='customers_gold_view',
    keys=['customer_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)