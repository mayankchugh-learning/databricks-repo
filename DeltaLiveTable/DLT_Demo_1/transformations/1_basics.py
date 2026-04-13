import dlt

# CREATE A STREAMING TABLE

@dlt.table(
    name = 'demo_stream_table'
)

def demo_stream_table():
    df = spark.readStream.table('databricksmayank.silver.sales_enr')
    return df

# CREATE MATERIALIZED VIEW

@dlt.table(
    name = 'demo_mat_view'
)

def demo_mat_view():
    df = spark.read.table('databricksmayank.silver.sales_enr')
    return df

# Temporary View (Batch)
@dlt.view(
    name = 'demo_temp_view'
)

def demo_temp_view():
    df = spark.read.table('databricksmayank.silver.sales_enr')
    return df

# Temporary View (Stream)
@dlt.view(
    name = 'demo_stream_view'
)

def demo_stream_view():
    df = spark.readStream.table('databricksmayank.silver.sales_enr')
    return df
