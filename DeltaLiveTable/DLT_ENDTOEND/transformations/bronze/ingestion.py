import dlt

# Ingesting SALES DATA
@dlt.table(
    name = 'sales_bronze'
)

def sales_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksmayank/bronze/bronze_volume/sales/")
    return df

# Ingesting STORES DATA
@dlt.table(
    name = 'stores_bronze'
)

def stores_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksmayank/bronze/bronze_volume/stores/")
    return df

# Ingesting PRODUCTS DATA
@dlt.table(
    name = 'products_bronze'
)

def products_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksmayank/bronze/bronze_volume/products/")
    return df

# Ingesting CUSTOMERS DATA
@dlt.table(
    name = 'customers_bronze'
)

def customers_bronze():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksmayank/bronze/bronze_volume/customers/")
    return df