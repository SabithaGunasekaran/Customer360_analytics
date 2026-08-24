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
# SILVER CUSTOMERS TRANSFORMATION
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_customers"
TARGET_TABLE = "silver_customers"


# ============================================================
# 1. READ BRONZE DATA
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze customer records: {df.count()}")


# ============================================================
# 2. CLEAN CUSTOMER ATTRIBUTES
# ============================================================
# Clean and standardize customer descriptive attributes.

df_silver = (
    df
    .withColumn(
        "customer_id",
        F.trim(F.col("customer_id"))
    )
    .withColumn(
        "customer_name",
        F.initcap(F.trim(F.col("customer_name")))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city")))
    )
    .withColumn(
        "province",
        F.upper(F.trim(F.col("province")))
    )
    .withColumn(
        "preferred_cuisine",
        F.initcap(F.trim(F.col("preferred_cuisine")))
    )
)


# ============================================================
# 3. STANDARDIZE POSTAL CODE
# ============================================================
# Convert postal code to uppercase and remove spaces.

df_silver = (
    df_silver
    .withColumn(
        "postal_code",
        F.upper(
            F.regexp_replace(
                F.trim(F.col("postal_code")),
                r"\s+",
                ""
            )
        )
    )
)


# ============================================================
# 4. CONVERT SIGNUP DATE
# ============================================================
# Convert signup date from string to DATE.

df_silver = (
    df_silver
    .withColumn(
        "signup_date",
        F.to_date(F.col("signup_date"))
    )
)


# ============================================================
# 5. DERIVE SIGNUP YEAR
# ============================================================
# Extract signup year for reporting.

df_silver = (
    df_silver
    .withColumn(
        "signup_year",
        F.year(F.col("signup_date"))
    )
)


# ============================================================
# 6. STANDARDIZE LOYALTY MEMBER FLAG
# ============================================================
# Convert source Y/YES/TRUE/1 values to Boolean.

df_silver = (
    df_silver
    .withColumn(
        "loyalty_member",
        F.when(
            F.upper(F.trim(F.col("loyalty_member"))).isin(
                "Y",
                "YES",
                "TRUE",
                "1"
            ),
            True
        )
        .otherwise(False)
    )
)


# ============================================================
# 7. STANDARDIZE MARKETING OPT-IN FLAG
# ============================================================
# Convert source Y/YES/TRUE/1 values to Boolean.

df_silver = (
    df_silver
    .withColumn(
        "marketing_opt_in",
        F.when(
            F.upper(F.trim(F.col("marketing_opt_in"))).isin(
                "Y",
                "YES",
                "TRUE",
                "1"
            ),
            True
        )
        .otherwise(False)
    )
)


# ============================================================
# 8. FILTER INVALID BUSINESS KEYS
# ============================================================
# Remove records without a valid customer ID.

df_silver = (
    df_silver
    .filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    )
)


# ============================================================
# 9. DEDUPLICATE CUSTOMERS
# ============================================================
# Keep the latest ingested record for each customer.

window_spec = (
    Window
    .partitionBy("customer_id")
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
# 10. ADD SILVER PROCESSING TIMESTAMP
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)


# ============================================================
# 11. SELECT FINAL COLUMNS
# ============================================================

df_silver = df_silver.select(
    "customer_id",
    "customer_name",
    "email",
    "phone",
    "city",
    "province",
    "postal_code",
    "signup_date",
    "signup_year",
    "preferred_cuisine",
    "loyalty_member",
    "marketing_opt_in",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)


# ============================================================
# 12. DATA PREVIEW
# ============================================================

print(f"Silver customer records: {df_silver.count()}")

display(
    df_silver.select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "city",
        "province",
        "preferred_cuisine",
        "loyalty_member",
        "marketing_opt_in"
    ).limit(20)
)


# ============================================================
# 13. WRITE SILVER TABLE
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)

print(f"Successfully loaded {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
