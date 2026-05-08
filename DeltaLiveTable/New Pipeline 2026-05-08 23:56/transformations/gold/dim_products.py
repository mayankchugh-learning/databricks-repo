import dlt
from pyspark.sql.functions import col

# GOLD Streamig Views on top of silver view (Not Silver Table)
@dlt.view(
    name = "products_gold_view"
)

def products_gold_view():
    df = spark.readStream.table("products_silver_view")
    return df

# CREATE DIM  SCD TYPE =2  TAble (With Auto CDC)
dlt.create_streaming_table(name="dim_products_scd2")

dlt.create_auto_cdc_flow(
    target='dim_products_scd2',
    source='products_gold_view',
    keys=['product_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)