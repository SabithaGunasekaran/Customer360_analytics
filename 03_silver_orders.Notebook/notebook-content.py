# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3bb7ae49-c088-477b-a108-cfbd531cbc5e",
# META       "default_lakehouse_name": "Food_Delivery_Lakehouse",
# META       "default_lakehouse_workspace_id": "49bfd15f-7448-470a-979e-3c6e71dd8c22",
# META       "known_lakehouses": [
# META         {
# META           "id": "3bb7ae49-c088-477b-a108-cfbd531cbc5e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# FOOD DELIVERY ANALYTICS - SILVER ORDERS
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_orders"
TARGET_TABLE = "silver_orders"


# ============================================================
# 1. READ BRONZE DATA
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze order records: {df.count()}")


# ============================================================
# 2. CLEAN BUSINESS KEYS
# ============================================================
# Remove leading/trailing spaces from IDs.

df_silver = (
    df
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("restaurant_id", F.trim(F.col("restaurant_id")))
)


# ============================================================
# 3. STANDARDIZE ORDER STATUS
# ============================================================
# Convert different source status values to standard values.

df_silver = (
    df_silver
    .withColumn(
        "order_status",
        F.when(
            F.upper(F.trim(F.col("order_status"))).isin(
                "COMPLETED",
                "DELIVERED"
            ),
            "Completed"
        )
        .when(
            F.upper(F.trim(F.col("order_status"))).isin(
                "CANCELLED",
                "CANCELED"
            ),
            "Cancelled"
        )
        .otherwise("In Progress")
    )
)


# ============================================================
# 4. STANDARDIZE PAYMENT METHOD
# ============================================================
# Standardize payment method formatting.

df_silver = (
    df_silver
    .withColumn(
        "payment_method",
        F.initcap(F.trim(F.col("payment_method")))
    )
)


# ============================================================
# 5. CONVERT DATA TYPES
# ============================================================
# Convert source strings to appropriate data types.

df_silver = (
    df_silver
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"))
    )
    .withColumn(
        "quantity",
        F.col("quantity").cast("int")
    )
    .withColumn(
        "unit_price",
        F.col("unit_price").cast("decimal(10,2)")
    )
)


# ============================================================
# 6. CALCULATE ORDER AMOUNT
# ============================================================
# Calculate total order amount.

df_silver = (
    df_silver
    .withColumn(
        "order_amount",
        (
            F.col("quantity") * F.col("unit_price")
        ).cast("decimal(12,2)")
    )
)


# ============================================================
# 7. DERIVE ORDER DATE ATTRIBUTES
# ============================================================
# Derive year and month information from order date.

df_silver = (
    df_silver
    .withColumn(
        "order_year",
        F.year(F.col("order_date"))
    )
    .withColumn(
        "order_month",
        F.month(F.col("order_date"))
    )
    .withColumn(
        "order_month_name",
        F.date_format(F.col("order_date"), "MMMM")
    )
)


# ============================================================
# 8. HIGH VALUE ORDER FLAG
# ============================================================
# Flag orders with an order amount of $100 or more.

df_silver = (
    df_silver
    .withColumn(
        "high_value_order_flag",
        F.when(
            F.col("order_amount") >= 100,
            True
        )
        .otherwise(False)
    )
)


# ============================================================
# 9. FILTER INVALID BUSINESS KEYS
# ============================================================
# Remove orders without required business keys.

df_silver = (
    df_silver
    .filter(
        F.col("order_id").isNotNull()
        & (F.col("order_id") != "")
        & F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
        & F.col("restaurant_id").isNotNull()
        & (F.col("restaurant_id") != "")
    )
)


# ============================================================
# 10. DEDUPLICATE ORDERS
# ============================================================
# Keep the latest ingested record for each order.

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last()
    )
)

df_silver = (
    df_silver
    .withColumn(
        "_row_num",
        F.row_number().over(window_spec)
    )
    .filter(
        F.col("_row_num") == 1
    )
    .drop("_row_num")
)


# ============================================================
# 11. ADD SILVER PROCESSING TIMESTAMP
# ============================================================
# Capture when the record was processed into Silver.

df_silver = (
    df_silver
    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)


# ============================================================
# 12. SELECT FINAL COLUMNS
# ============================================================

df_silver = df_silver.select(
    "order_id",
    "customer_id",
    "restaurant_id",
    "order_date",
    "order_year",
    "order_month",
    "order_month_name",
    "quantity",
    "unit_price",
    "order_amount",
    "high_value_order_flag",
    "order_status",
    "payment_method",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)


# ============================================================
# 13. WRITE SILVER TABLE
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


# ============================================================
# 14. VERIFY LOAD
# ============================================================

print(f"Successfully loaded {TARGET_TABLE}")
print(f"Silver order records: {df_silver.count()}")

display(df_silver.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
