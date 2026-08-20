# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e751b088-da93-4352-b14f-ea1d39293c08",
# META       "default_lakehouse_name": "Customer360_lakehouse",
# META       "default_lakehouse_workspace_id": "48394f03-b48a-435e-a47e-23b7330aa2e4",
# META       "known_lakehouses": [
# META         {
# META           "id": "e751b088-da93-4352-b14f-ea1d39293c08"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Customer 360 - Silver Marketing Interactions Transformation
# Microsoft Fabric Lakehouse / PySpark

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_marketing_interactions"
TARGET_TABLE = "silver_marketing_interactions"
INVALID_TABLE = "silver_marketing_interactions_invalid"


# ============================================================
# 1. READ BRONZE
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze marketing interaction records: {df.count()}")


# ============================================================
# 2. PRESERVE RAW VALUES FOR DATA QUALITY VALIDATION
# ============================================================

df_silver = (
    df
    .withColumn("_raw_interaction_date", F.col("interaction_date"))
    .withColumn("_raw_created_date", F.col("created_date"))
    .withColumn("_raw_updated_date", F.col("updated_date"))
)


# ============================================================
# 3. CLEAN BUSINESS KEYS
# ============================================================

df_silver = (
    df_silver
    .withColumn("interaction_id", F.trim(F.col("interaction_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("interaction_id", F.when(F.col("interaction_id") == "", F.lit(None)).otherwise(F.col("interaction_id")))
    .withColumn("customer_id", F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
)


# ============================================================
# 4. CLEAN DESCRIPTIVE COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("channel", F.initcap(F.trim(F.col("channel"))))
    .withColumn("campaign", F.initcap(F.trim(F.col("campaign"))))
    .withColumn("interaction_type", F.initcap(F.trim(F.col("interaction_type"))))
    .withColumn("response", F.initcap(F.trim(F.col("response"))))
)


# ============================================================
# 5. CONVERT DATE COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("interaction_date", F.to_date(F.col("interaction_date")))
    .withColumn("created_date", F.to_date(F.col("created_date")))
    .withColumn("updated_date", F.to_date(F.col("updated_date")))
)


# ============================================================
# 6. STANDARDIZE CHANNEL
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "channel",
        F.when(F.upper(F.col("channel")).isin("EMAIL", "E-MAIL"), "Email")
         .when(F.upper(F.col("channel")).isin("SMS", "TEXT"), "SMS")
         .when(F.upper(F.col("channel")).isin("SOCIAL", "SOCIAL MEDIA"), "Social Media")
         .when(F.upper(F.col("channel")).isin("WEB", "WEBSITE"), "Web")
         .otherwise("Other")
    )
)


# ============================================================
# 7. STANDARDIZE RESPONSE
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "response",
        F.when(F.upper(F.col("response")).isin("YES", "Y", "RESPONDED"), "Responded")
         .when(F.upper(F.col("response")).isin("NO", "N", "NO RESPONSE"), "No Response")
         .otherwise("Unknown")
    )
)


# ============================================================
# 8. HANDLE NULL / EMPTY DESCRIPTIVE VALUES
# ============================================================

df_silver = (
    df_silver
    .withColumn("campaign", F.when(F.col("campaign").isNull() | (F.trim(F.col("campaign")) == ""), "Unknown").otherwise(F.col("campaign")))
    .withColumn("interaction_type", F.when(F.col("interaction_type").isNull() | (F.trim(F.col("interaction_type")) == ""), "Unknown").otherwise(F.col("interaction_type")))
)


# ============================================================
# 9. RESPONSE FLAG
# ============================================================

df_silver = df_silver.withColumn("responded_flag", F.when(F.col("response") == "Responded", True).otherwise(False))


# ============================================================
# 10. DIGITAL CHANNEL FLAG
# ============================================================

df_silver = df_silver.withColumn("digital_channel_flag", F.when(F.col("channel").isin("Email", "SMS", "Social Media", "Web"), True).otherwise(False))


# ============================================================
# 11. DERIVE INTERACTION YEAR / MONTH
# ============================================================

df_silver = (
    df_silver
    .withColumn("interaction_year", F.year(F.col("interaction_date")))
    .withColumn("interaction_month", F.month(F.col("interaction_date")))
    .withColumn("interaction_month_name", F.date_format(F.col("interaction_date"), "MMMM"))
)


# ============================================================
# 12. DERIVE INTERACTION RECENCY
# ============================================================

df_silver = df_silver.withColumn("days_since_interaction", F.datediff(F.current_date(), F.col("interaction_date")))


# ============================================================
# 13. INTERACTION RECENCY BAND
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "interaction_recency_band",
        F.when(F.col("days_since_interaction").isNull(), "Unknown")
         .when(F.col("days_since_interaction") <= 30, "Last 30 Days")
         .when(F.col("days_since_interaction") <= 90, "31-90 Days")
         .when(F.col("days_since_interaction") <= 180, "91-180 Days")
         .otherwise("180+ Days")
    )
)


# ============================================================
# 14. DATA QUALITY VALIDATION
# ============================================================
# Critical errors are evaluated first.
# Unknown channel / response are retained as warnings.

df_silver = (
    df_silver
    .withColumn(
        "data_quality_status",
        F.when(F.col("interaction_id").isNull(), "Invalid - Missing Interaction ID")
         .when(F.col("customer_id").isNull(), "Invalid - Missing Customer ID")
         .when(F.col("_raw_interaction_date").isNotNull() & (F.trim(F.col("_raw_interaction_date")) != "") & F.col("interaction_date").isNull(), "Invalid - Invalid Interaction Date Format")
         .when(F.col("interaction_date").isNull(), "Invalid - Missing Interaction Date")
         .when(F.col("interaction_date") > F.current_date(), "Invalid - Future Interaction Date")
         .when(F.col("_raw_created_date").isNotNull() & (F.trim(F.col("_raw_created_date")) != "") & F.col("created_date").isNull(), "Invalid - Invalid Created Date Format")
         .when(F.col("_raw_updated_date").isNotNull() & (F.trim(F.col("_raw_updated_date")) != "") & F.col("updated_date").isNull(), "Invalid - Invalid Updated Date Format")
         .when(F.col("created_date").isNotNull() & (F.col("created_date") > F.current_date()), "Invalid - Future Created Date")
         .when(F.col("updated_date").isNotNull() & F.col("created_date").isNotNull() & (F.col("updated_date") < F.col("created_date")), "Invalid - Updated Date Before Created Date")
         .when(F.col("channel") == "Other", "Warning - Unknown Channel")
         .when(F.col("response") == "Unknown", "Warning - Unknown Response")
         .otherwise("Valid")
    )
)


# ============================================================
# 15. ADD SILVER PROCESSING TIMESTAMP
# ============================================================

df_silver = df_silver.withColumn("_silver_processed_timestamp", F.current_timestamp())


# ============================================================
# 16. SPLIT INVALID AND ACCEPTED RECORDS
# ============================================================

df_invalid = df_silver.filter(F.col("data_quality_status").startswith("Invalid"))

df_accepted = df_silver.filter(~F.col("data_quality_status").startswith("Invalid"))


# ============================================================
# 17. DEDUPLICATE ACCEPTED RECORDS
# ============================================================
# Latest updated_date wins.
# Latest ingestion timestamp is used as the next tie-breaker.

window_spec = (
    Window
    .partitionBy("interaction_id")
    .orderBy(
        F.col("updated_date").desc_nulls_last(),
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("interaction_date").desc_nulls_last()
    )
)

df_silver = (
    df_accepted
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)


# ============================================================
# 18. FINAL COLUMN ORDER
# ============================================================

silver_columns = [
    "interaction_id",
    "customer_id",
    "interaction_date",
    "channel",
    "campaign",
    "interaction_type",
    "response",
    "responded_flag",
    "digital_channel_flag",
    "interaction_year",
    "interaction_month",
    "interaction_month_name",
    "days_since_interaction",
    "interaction_recency_band",
    "created_date",
    "updated_date",
    "data_quality_status",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
]

df_silver = df_silver.select(*silver_columns)

df_invalid = df_invalid.select(*silver_columns)


# ============================================================
# 19. DATA QUALITY SUMMARY
# ============================================================

bronze_count = df.count()
accepted_count = df_silver.count()
invalid_count = df_invalid.count()

print("==========================================")
print("MARKETING INTERACTIONS SILVER LOAD SUMMARY")
print("==========================================")
print(f"Bronze records          : {bronze_count}")
print(f"Silver accepted records : {accepted_count}")
print(f"Silver invalid records  : {invalid_count}")

print("\nAccepted Data Quality Summary:")
df_silver.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nInvalid Record Summary:")
df_invalid.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nChannel Summary:")
df_silver.groupBy("channel").count().orderBy("channel").show()

print("\nResponse Summary:")
df_silver.groupBy("response").count().orderBy("response").show()

print("\nRecency Band Summary:")
df_silver.groupBy("interaction_recency_band").count().orderBy("interaction_recency_band").show()


# ============================================================
# 20. WRITE ACCEPTED SILVER TABLE
# ============================================================

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

print(f"Successfully loaded {TARGET_TABLE}")


# ============================================================
# 21. WRITE INVALID SILVER TABLE
# ============================================================

df_invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(INVALID_TABLE)

print(f"Successfully loaded {INVALID_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
