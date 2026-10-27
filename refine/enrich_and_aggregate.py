# Databricks notebook source
"""
Task
1. Create raw tables for each source dataset
2. Create an enriched table for customers and products
3. Create an enriched table which has
   1. order information
      1. Profit rounded to 2 decimal places
   2. Customer name and country
   3. Product category and sub category
4. Create an aggregate table that shows profit by
   1. Year
   2. Product Category
   3. Product Sub Category
   4. Customer
5. Using SQL output the following aggregates
   1. Profit by Year
   2. Profit by Year + Product Category
   3. Profit by Customer
   4. Profit by Customer + Year

"""

# COMMAND ----------

"""Task 1 completed in src/raw/source_parser.
Right now I did bit of generic naming convention considering it to be onetime"""

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

"""
Table writer function, created a common function to resuse."""


def table_writer(df, table_name: str, mode: str = "overwrite", merge_schema: bool = True):
    """Create Delta table."""
    (df.write
       .format("delta")
       .mode(mode)
       .option("mergeSchema", str(merge_schema).lower())
       .saveAsTable(table_name))


# Task 2: Create Enriched Tables for Customers and Products with Cleaned Customer Names
def process_master_tables(raw_db: str = "raw.default", refine_db: str = "refine.default"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {refine_db}")
    
    # Clean customer_name by trimming whitespace and special padding, as I noticed some DQ issue in the aggregated tables.
    df_customer = (
        spark.table(f"{raw_db}.customer")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_name", F.trim(F.regexp_replace(F.col("customer_name"), r"[^\w\s]", "")))
    )
    table_writer(df_customer, f"{refine_db}.customers_enriched")
    
    df_product = spark.table(f"{raw_db}.products").dropDuplicates(["product_id"])
    table_writer(df_product, f"{refine_db}.products_enriched")
    
    print("Enriched customer and product tables created.")


# Task 3: Create Enriched Orders Table
def process_enriched_orders(raw_db: str = "raw.default", refine_db: str = "refine.default"):
    df_orders = spark.table(f"{raw_db}.orders")

    df_customers = (
        spark.table(f"{refine_db}.customers_enriched")
        .select("customer_id", "customer_name", "country")
        .dropDuplicates(["customer_id"])
    )

    df_products = (
        spark.table(f"{refine_db}.products_enriched")
        .select("product_id", "category", "sub_category")
        .dropDuplicates(["product_id"])
    )

    for column in ["customer_name", "country", "category", "sub_category"]:
        if column in df_orders.columns:
            df_orders = df_orders.drop(column)

    df_orders_enriched = (
        df_orders
        .join(df_customers, on="customer_id", how="left")
        .join(df_products, on="product_id", how="left")
        .withColumn("profit_rounded", F.round(F.col("profit").cast("double"), 2))
        .withColumn("parsed_order_date", F.try_to_timestamp(F.col("order_date").cast("string"), F.lit("d/M/yyyy")))
    )

    table_writer(df_orders_enriched, f"{refine_db}.orders_enriched")
    print("Enriched orders table created.")


# Task 4: Create Aggregate Table with added additional column > profit status. As there was data quality issues identified with negative ststus implying loss?
def process_aggregated_table(refine_db: str = "refine.default"):
    df_aggregated = (
        spark.table(f"{refine_db}.orders_enriched")
        .withColumn("order_year", F.year("parsed_order_date"))
        .groupBy("order_year", "category", "sub_category", "customer_name")
        .agg(F.round(F.sum("profit_rounded"), 2).alias("total_profit"))
        .withColumn(
            "profit_status", 
            F.when(F.col("total_profit") < 0, F.lit("NEGATIVE PROFIT WARNING"))
             .otherwise(F.lit("NORMAL"))
        )
    )
    table_writer(df_aggregated, f"{refine_db}.profit_aggregated")
    print("Aggregated profit table created with profit status notifications.")


# Audit Summary - defined some qualitative tests based on daa from the aggregared tables.
def process_dq_audit_summary(refine_db: str = "refine.default"):
    """Computes key DQ rule violations including customer_name null checks per table."""
    
    # 1. Customers DQ Metrics (checking customer_id and customer_name nulls)
    df_cust = spark.table(f"{refine_db}.customers_enriched")
    cust_metrics = df_cust.agg(
        F.lit("customers_enriched").alias("table_name"),
        F.count("*").alias("total_rows"),
        F.sum(F.col("customer_id").isNull().cast("long")).alias("failed_null_customer_id"),
        F.sum(F.col("customer_name").isNull().cast("long")).alias("failed_null_customer_name"),
        F.sum(F.col("country").isNull().cast("long")).alias("failed_null_country")
    )

    # 2. Products DQ Metrics
    df_prod = spark.table(f"{refine_db}.products_enriched")
    prod_metrics = df_prod.agg(
        F.lit("products_enriched").alias("table_name"),
        F.count("*").alias("total_rows"),
        F.sum(F.col("product_id").isNull().cast("long")).alias("failed_null_customer_id"),
        F.lit(0).alias("failed_null_customer_name"),
        F.sum(F.col("category").isNull().cast("long")).alias("failed_null_country")
    )

    # 3. Orders DQ Metrics (checking customer_id, customer_name, and profit nulls)
    df_ord = spark.table(f"{refine_db}.orders_enriched")
    ord_metrics = df_ord.agg(
        F.lit("orders_enriched").alias("table_name"),
        F.count("*").alias("total_rows"),
        F.sum(F.col("customer_id").isNull().cast("long")).alias("failed_null_customer_id"),
        F.sum(F.col("customer_name").isNull().cast("long")).alias("failed_null_customer_name"),
        F.sum(F.col("profit_rounded").isNull().cast("long")).alias("failed_null_country")
    )

    # audit metrics
    dq_summary_df = cust_metrics.unionByName(prod_metrics).unionByName(ord_metrics).withColumn("audit_timestamp", F.current_timestamp())
    
    table_writer(dq_summary_df, f"{refine_db}.dq_audit_summary")
    print("DQ Audit Summary.")
    dq_summary_df.display()


# Executor
try:
    process_master_tables(raw_db="raw.default", refine_db="refine.default")
except Exception as e:
    print(f"Error in process_master_tables: {str(e)}")

try:
    process_enriched_orders(raw_db="raw.default", refine_db="refine.default")
except Exception as e:
    print(f"Error in process_enriched_orders: {str(e)}")

try:
    process_aggregated_table(refine_db="refine.default")
except Exception as e:
    print(f"Error in process_aggregated_table: {str(e)}")

try:
    process_dq_audit_summary(refine_db="refine.default")
except Exception as e:
    print(f"Error in process_dq_audit_summary: {str(e)}")

# COMMAND ----------

"""
Basic unit tests with assertions have been written here in "PEIAssesment/src/test".
We can define more qualitative and quantitative tests based on the business and technical requirements.
But rightnow I have added tests to have MVP."""
