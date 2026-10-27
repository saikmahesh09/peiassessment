# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ensure spark explicitly points to the active SparkSession and avoids namespace conflicts
spark = SparkSession.builder.getOrCreate()



#Master Table Deduplication Tests
def test_master_table_deduplication():
    data = [("C001", "Alice", "USA"), ("C001", "Alice", "USA"), ("C002", "Bob", "UK")]
    df = spark.createDataFrame(data, ["customer_id", "customer_name", "country"])
    
    deduped_df = df.dropDuplicates(["customer_id"])
    
    assert deduped_df.count() == 2, f"Expected 2 rows, got {deduped_df.count()}"
    ids = [row["customer_id"] for row in deduped_df.collect()]
    assert sorted(ids) == ["C001", "C002"], f"Expected IDs ['C001', 'C002'], got {sorted(ids)}"
    print("PASSED: test_master_table_deduplication")


#Enriched Orders: Profit Rounding & Edge Cases
def test_enriched_orders_profit_rounding():
    data = [("O1", "C001", "P001", 123.4567, "15/05/2023"),
            ("O2", "C002", "P002", -5.4321, "20/10/2023"),
            ("O3", "C001", "P001", 0.0, "invalid_date")]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "product_id", "profit", "order_date"])

    parsed_date_col = F.try_to_timestamp(F.col("order_date").cast("string"), F.lit("d/M/yyyy"))

    result_df = (
        df.withColumn("profit_rounded", F.round(F.col("profit").cast("double"), 2))
          .withColumn("parsed_order_date", parsed_date_col)
          .withColumn("dq_null_order_date", F.col("parsed_order_date").isNull())
    )

    rows = {r["order_id"]: r for r in result_df.collect()}
    assert rows["O1"]["profit_rounded"] == 123.46, f"Expected 123.46, got {rows['O1']['profit_rounded']}"
    assert rows["O2"]["profit_rounded"] == -5.43, f"Expected -5.43, got {rows['O2']['profit_rounded']}"
    assert rows["O3"]["dq_null_order_date"] is True, "Expected invalid date to trigger null flag"
    print("PASSED: test_enriched_orders_profit_rounding")


#Data Quality: Future Date Validation
def test_future_date_dq_rule():
    data = [("O1", "2020-01-01"), ("O2", "2099-12-31")]
    df = spark.createDataFrame(data, ["order_id", "order_date_str"])

    df_checked = df.withColumn("is_future_date", F.to_date("order_date_str") > F.current_date())
    
    rows = {r["order_id"]: r["is_future_date"] for r in df_checked.collect()}
    assert rows["O1"] is False, "O1 should not be a future date"
    assert rows["O2"] is True, "O2 should be detected as a future date"
    print("PASSED: test_future_date_dq_rule")


#Aggregation Logic Test
def test_aggregation_grouping_and_sum():
    data = [
        (2023, "Furniture", "Chairs", "Alice", 10.50),
        (2023, "Furniture", "Chairs", "Alice", 5.25),
        (2023, "Technology", "Phones", "Bob", 100.00)
    ]
    columns = ["order_year", "category", "sub_category", "customer_name", "profit_rounded"]
    df = spark.createDataFrame(data, columns)

    agg_df = (
        df.groupBy("order_year", "category", "sub_category", "customer_name")
          .agg(F.round(F.sum("profit_rounded"), 2).alias("total_profit"))
    )

    rows = agg_df.collect()
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

    alice_row = [r for r in rows if r["customer_name"] == "Alice"][0]
    assert alice_row["total_profit"] == 15.75, f"Expected 15.75, got {alice_row['total_profit']}"
    print("PASSED: test_aggregation_grouping_and_sum")


# Notebook Execution Runner with Try-Catch Blocks
print("Running Pipeline Unit Tests in Databricks Notebook...")

tests = [
    test_column_sanitization_edge_cases,
    test_master_table_deduplication,
    test_enriched_orders_profit_rounding,
    test_future_date_dq_rule,
    test_aggregation_grouping_and_sum
]

passed_count = 0
failed_count = 0

for test_func in tests:
    try:
        test_func()
        passed_count += 1
    except Exception as e:
        failed_count += 1
        print(f"FAILED: {test_func.__name__} -> {str(e)}")

print(f"Test Summary: {passed_count} Passed, {failed_count} Failed.")