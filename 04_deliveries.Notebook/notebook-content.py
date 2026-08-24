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
# FOOD DELIVERY ANALYTICS
# SILVER DELIVERIES TRANSFORMATION
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_deliveries"
TARGET_TABLE = "silver_deliveries"


# ============================================================
# 1. READ BRONZE DATA
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze delivery records: {df.count()}")


# ============================================================
# 2. CLEAN BUSINESS KEYS
# ============================================================
# Remove leading and trailing spaces from IDs.

df_silver = (
    df
    .withColumn(
        "delivery_id",
        F.trim(F.col("delivery_id"))
    )
    .withColumn(
        "order_id",
        F.trim(F.col("order_id"))
    )
)


# ============================================================
# 3. STANDARDIZE DELIVERY STATUS
# ============================================================
# Convert different source values to common delivery statuses.

df_silver = (
    df_silver
    .withColumn(
        "delivery_status",
        F.when(
            F.upper(F.trim(F.col("delivery_status"))).isin(
                "DELIVERED",
                "COMPLETE",
                "COMPLETED"
            ),
            "Delivered"
        )
        .when(
            F.upper(F.trim(F.col("delivery_status"))).isin(
                "CANCELLED",
                "CANCELED"
            ),
            "Cancelled"
        )
        .otherwise("In Transit")
    )
)


# ============================================================
# 4. CONVERT DATA TYPES
# ============================================================
# Convert delivery timestamps and driver rating.

df_silver = (
    df_silver
    .withColumn(
        "delivery_start_time",
        F.to_timestamp(F.col("delivery_start_time"))
    )
    .withColumn(
        "delivery_end_time",
        F.to_timestamp(F.col("delivery_end_time"))
    )
    .withColumn(
        "driver_rating",
        F.col("driver_rating").cast("decimal(3,1)")
    )
)


# ============================================================
# 5. CALCULATE DELIVERY TIME
# ============================================================
# Calculate elapsed delivery time in minutes.

df_silver = (
    df_silver
    .withColumn(
        "delivery_minutes",
        F.when(
            F.col("delivery_end_time").isNotNull(),
            (
                F.unix_timestamp(F.col("delivery_end_time"))
                - F.unix_timestamp(F.col("delivery_start_time"))
            ) / 60
        ).cast("int")
    )
)


# ============================================================
# 6. FILTER INVALID BUSINESS KEYS
# ============================================================
# Delivery and Order IDs are required.

df_silver = (
    df_silver
    .filter(
        F.col("delivery_id").isNotNull()
        & (F.col("delivery_id") != "")
        & F.col("order_id").isNotNull()
        & (F.col("order_id") != "")
    )
)


# ============================================================
# 7. DEDUPLICATE
# ============================================================
# Keep the latest delivery record.

window_spec = (
    Window
    .partitionBy("delivery_id")
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
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)


# ============================================================
# 8. ADD PROCESSING TIMESTAMP
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)


# ============================================================
# 9. SELECT FINAL COLUMNS
# ============================================================

df_silver = df_silver.select(
    "delivery_id",
    "order_id",
    "delivery_status",
    "delivery_start_time",
    "delivery_end_time",
    "delivery_minutes",
    "driver_rating",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)


# ============================================================
# 10. WRITE SILVER TABLE
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully loaded {TARGET_TABLE}")
print(f"Silver delivery records: {df_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

