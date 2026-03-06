from pyspark.sql import SparkSession
from pyspark.sql import functions as F



# ------------------------------------
# Read Silver Layer
# ------------------------------------
silver_df = spark.read.parquet("/opt/spark-data/silver/retail_sales_clean.parquet")

# ------------------------------------
# Derived Column: total_amount
# ------------------------------------
silver_df = silver_df.withColumn(
    "total_amount",
    F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
)

# ------------------------------------
# 1️⃣ Daily Sales Metrics
# ------------------------------------
daily_sales_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("total_orders"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value")
    )
)

daily_sales_df.write.mode("overwrite").parquet("/opt/spark-data/gold/daily_sales_metrics.parquet")

# ------------------------------------
# 2️⃣ Product Category Performance
# ------------------------------------
product_perf_df = (
    silver_df
    .groupBy("product_category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("category_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.count("transaction_id").alias("order_count")
    )
)

product_perf_df.write.mode("overwrite").parquet("/opt/spark-data/gold/product_category_performance.parquet")

# ------------------------------------
# 3️⃣ City-Level Revenue Metrics
# ------------------------------------
city_revenue_df = (
    silver_df
    .groupBy("city", "state")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("city_revenue"),
        F.count("transaction_id").alias("order_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value")
    )
)

city_revenue_df.write.mode("overwrite").parquet("/opt/spark-data/gold/city_revenue_metrics.parquet")

print("✅ Gold layer created successfully")
