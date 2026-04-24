import dlt
from pyspark.sql.functions import *

# Note: Using scd_stg staging table defined in scd.py
# No need to redefine it here - just reference it in the Auto CDC flow

# CREATE AN EMPTY STREAMING TABLE FOR SCD TYPE 2
dlt.create_streaming_table(
    name = "scd3_table"
)

# AUTO CDC FLOW - reads from shared scd_stg staging table
dlt.create_auto_cdc_flow(
  target = "scd3_table",
  source = "scd_stg",  # References the staging table from scd.py
  keys = ["product_id"],
  sequence_by = col("processDate"),
  stored_as_scd_type = 2,
  except_column_list = ["processDate"]
)