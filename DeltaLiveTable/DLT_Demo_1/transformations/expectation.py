import dlt

table_name = "databricksmayank.silver.product_enr"

expectation = {
    "rule1": "product_id is not null",
    "rule2": "category is not null"
}

@dlt.table(
    name = "except_table"
)
@dlt.expect_all_or_drop(expectation)
def except_table():
    df = spark.read.table(f"{table_name}")
    return df