from pyspark.sql import SparkSession
from pyspark.sql import functions as F




# ------------------------------------
# Read Bronze Layer
# ------------------------------------
bronze_df = spark.read.parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")
print("Bronze count:", bronze_df.count())
bronze_df.printSchema()

# ------------------------------------
# Deduplicate Transactions
# ------------------------------------
bronze_df.groupBy("transaction_id").count().filter(F.col("count") > 1).show(5, truncate=False)
silver_df = bronze_df.dropDuplicates(["transaction_id"])
print("After deduplication, Silver count:", silver_df.count())

# ------------------------------------
# Date Corrections
# ------------------------------------
silver_df = silver_df.withColumn(
    "ship_date",
    F.when(F.col("ship_date") < F.col("order_date"), None)
     .otherwise(F.col("ship_date"))
)

# ------------------------------------
# Quantity & Price Cleaning
# ------------------------------------
silver_df = silver_df.filter(F.col("quantity") > 0)
silver_df = silver_df.withColumn(
    "unit_price",
    F.when(F.col("unit_price") <= 0, None).otherwise(F.col("unit_price"))
)

# ------------------------------------
# Discount Cleaning
# ------------------------------------
silver_df = silver_df.withColumn(
    "discount_pct",
    F.when((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100), None)
     .otherwise(F.col("discount_pct"))
)

# ------------------------------------
# Customer Age Cleaning
# ------------------------------------
silver_df = silver_df.withColumn(
    "customer_age",
    F.when((F.col("customer_age") < 15) | (F.col("customer_age") > 100), None)
     .otherwise(F.col("customer_age"))
)

# ------------------------------------
# Standardize Gender
# ------------------------------------
silver_df = silver_df.withColumn(
    "gender",
    F.when(F.upper(F.trim(F.col("gender"))) == "MALE", "M")
     .when(F.upper(F.trim(F.col("gender"))) == "FEMALE", "F")
     .when(F.col("gender").isin("M", "F"), F.col("gender"))
     .otherwise(None)
)

# ------------------------------------
# Standardize Payment Type
# ------------------------------------
silver_df = silver_df.withColumn(
    "payment_type",
    F.when(F.col("payment_type").isin("Card", "UPI", "COD"), F.col("payment_type"))
     .otherwise(None)
)

print("Silver count after cleaning:", silver_df.count())

# ------------------------------------
# Derived Column: total_amount
# ------------------------------------
silver_df = silver_df.withColumn(
    "total_amount",
    F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
)

# ------------------------------------
# Write Silver Layer
# ------------------------------------
(
    silver_df
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/silver/retail_sales_clean.parquet")
)

print("✅ Silver layer created successfully")
