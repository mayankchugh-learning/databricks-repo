import dlt

# Create Empty Streaming table 
dlt.create_streaming_table(
    name="append_table"
    )


# Create Streaming table  (flow 1)
@dlt.append_flow(
    target="append_table"
    )
def flow_1():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksmayank/bronze/autovol/flow1/")
    return df

# Create Streaming table  (flow 2)
@dlt.append_flow(
    target="append_table"
    )
def flow_2():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksmayank/bronze/autovol/flow2/")
    return df