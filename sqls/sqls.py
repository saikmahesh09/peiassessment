# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Profit by Year
# MAGIC SELECT order_year, SUM(total_profit) AS profit 
# MAGIC FROM refine.default.profit_aggregated 
# MAGIC GROUP BY order_year 
# MAGIC ORDER BY order_year;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Profit by Year + Product Category
# MAGIC SELECT order_year, category, SUM(total_profit) AS profit 
# MAGIC FROM refine.default.profit_aggregated 
# MAGIC GROUP BY order_year, category 
# MAGIC ORDER BY order_year, category;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Profit by Customer
# MAGIC SELECT customer_name, SUM(total_profit) AS profit 
# MAGIC FROM refine.default.profit_aggregated 
# MAGIC GROUP BY customer_name 
# MAGIC ORDER BY profit DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Profit by Customer + Year
# MAGIC SELECT customer_name, order_year, SUM(total_profit) AS profit 
# MAGIC FROM refine.default.profit_aggregated 
# MAGIC GROUP BY customer_name, order_year 
# MAGIC ORDER BY customer_name, order_year;