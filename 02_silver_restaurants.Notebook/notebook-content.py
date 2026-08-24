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
# SILVER RESTAURANTS TRANSFORMATION
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_restaurants"
TARGET_TABLE = "silver_restaurants"


# ============================================================
# 1. READ BRONZE DATA
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze restaurant records: {df.count()}")


# ============================================================
# 2. CLEAN AND STANDARDIZE STRINGS
# ============================================================
# Standardize restaurant, cuisine and city values.

df_silver = (
    df
    .withColumn("restaurant_id", F.trim(F.col("restaurant_id")))
    .withColumn("restaurant_name", F.initcap(F.trim(F.col("restaurant_name"))))
    .withColumn("cuisine_type", F.initcap(F.trim(F.col("cuisine_type"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
)


# ============================================================
# 3. CONVERT RATING
# ============================================================
# Convert restaurant rating to numeric data type.

df_silver = (
    df_silver
    .withColumn(
        "restaurant_rating",
        F.col("restaurant_rating").cast("decimal(3,1)")
    )
)


# ============================================================
# 4. FILTER INVALID RESTAURANT IDS
# ============================================================
# Restaurant ID is required as the business key.

df_silver = (
    df_silver
    .filter(
        F.col("restaurant_id").isNotNull()
        & (F.col("restaurant_id") != "")
    )
)


# ============================================================
# 5. DEDUPLICATE
# ============================================================
# Keep the latest restaurant record.

window_spec = (
    Window
    .partitionBy("restaurant_id")
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
# 6. ADD PROCESSING TIMESTAMP
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)


# ============================================================
# 7. SELECT FINAL COLUMNS
# ============================================================

df_silver = df_silver.select(
    "restaurant_id",
    "restaurant_name",
    "cuisine_type",
    "city",
    "restaurant_rating",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)


# ============================================================
# 8. WRITE SILVER TABLE
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully loaded {TARGET_TABLE}")
print(f"Silver restaurant records: {df_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
