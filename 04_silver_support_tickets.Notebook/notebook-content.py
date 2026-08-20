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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Customer 360 - Silver Support Tickets Transformation
# Microsoft Fabric Lakehouse / PySpark

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_support_tickets"
TARGET_TABLE = "silver_support_tickets"
INVALID_TABLE = "silver_support_tickets_invalid"


# ============================================================
# 1. READ BRONZE
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze support ticket records: {df.count()}")


# ============================================================
# 2. PRESERVE RAW VALUES FOR DATA QUALITY VALIDATION
# ============================================================

df_silver = (
    df
    .withColumn("_raw_ticket_date", F.col("ticket_date"))
    .withColumn("_raw_resolution_date", F.col("resolution_date"))
    .withColumn("_raw_created_date", F.col("created_date"))
    .withColumn("_raw_updated_date", F.col("updated_date"))
)


# ============================================================
# 3. CLEAN BUSINESS KEYS
# ============================================================

df_silver = (
    df_silver
    .withColumn("ticket_id", F.trim(F.col("ticket_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("ticket_id", F.when(F.col("ticket_id") == "", F.lit(None)).otherwise(F.col("ticket_id")))
    .withColumn("customer_id", F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
)


# ============================================================
# 4. CLEAN DESCRIPTIVE COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("priority", F.initcap(F.trim(F.col("priority"))))
    .withColumn("status", F.initcap(F.trim(F.col("status"))))
    .withColumn("description", F.trim(F.col("description")))
)


# ============================================================
# 5. STANDARDIZE PRIORITY
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "priority",
        F.when(F.upper(F.col("priority")).isin("H", "HIGH"), "High")
         .when(F.upper(F.col("priority")).isin("M", "MEDIUM"), "Medium")
         .when(F.upper(F.col("priority")).isin("L", "LOW"), "Low")
         .otherwise("Unknown")
    )
)


# ============================================================
# 6. STANDARDIZE STATUS
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "status",
        F.when(F.upper(F.col("status")).isin("OPEN", "NEW"), "Open")
         .when(F.upper(F.col("status")).isin("IN PROGRESS", "IN_PROGRESS"), "In Progress")
         .when(F.upper(F.col("status")).isin("CLOSED", "RESOLVED"), "Closed")
         .otherwise("Unknown")
    )
)


# ============================================================
# 7. HANDLE NULL / EMPTY DESCRIPTIVE VALUES
# ============================================================

df_silver = (
    df_silver
    .withColumn("category", F.when(F.col("category").isNull() | (F.trim(F.col("category")) == ""), "Unknown").otherwise(F.col("category")))
    .withColumn("description", F.when(F.col("description").isNull() | (F.trim(F.col("description")) == ""), "No Description").otherwise(F.col("description")))
)


# ============================================================
# 8. CONVERT DATE COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("ticket_date", F.to_date(F.col("ticket_date")))
    .withColumn("resolution_date", F.to_date(F.col("resolution_date")))
    .withColumn("created_date", F.to_date(F.col("created_date")))
    .withColumn("updated_date", F.to_date(F.col("updated_date")))
)


# ============================================================
# 9. OPEN TICKET FLAG
# ============================================================

df_silver = df_silver.withColumn("is_open_ticket", F.when(F.col("status").isin("Open", "In Progress"), True).otherwise(False))


# ============================================================
# 10. RESOLUTION DAYS
# ============================================================

df_silver = df_silver.withColumn("resolution_days", F.when(F.col("resolution_date").isNotNull(), F.datediff(F.col("resolution_date"), F.col("ticket_date"))))


# ============================================================
# 11. TICKET AGE
# ============================================================

df_silver = df_silver.withColumn("ticket_age_days", F.when(F.col("resolution_date").isNull(), F.datediff(F.current_date(), F.col("ticket_date"))).otherwise(F.datediff(F.col("resolution_date"), F.col("ticket_date"))))


# ============================================================
# 12. SLA TARGET
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "sla_target_days",
        F.when(F.col("priority") == "High", 1)
         .when(F.col("priority") == "Medium", 3)
         .when(F.col("priority") == "Low", 5)
         .otherwise(7)
    )
)


# ============================================================
# 13. SLA BREACH FLAG
# ============================================================

df_silver = df_silver.withColumn("sla_breached_flag", F.when(F.col("ticket_age_days") > F.col("sla_target_days"), True).otherwise(False))


# ============================================================
# 14. TICKET AGE BAND
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "ticket_age_band",
        F.when(F.col("ticket_age_days").isNull(), "Unknown")
         .when(F.col("ticket_age_days") <= 1, "0-1 Days")
         .when(F.col("ticket_age_days") <= 3, "2-3 Days")
         .when(F.col("ticket_age_days") <= 7, "4-7 Days")
         .otherwise("8+ Days")
    )
)


# ============================================================
# 15. DATA QUALITY VALIDATION
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "data_quality_status",
        F.when(F.col("ticket_id").isNull(), "Invalid - Missing Ticket ID")
         .when(F.col("customer_id").isNull(), "Invalid - Missing Customer ID")
         .when(F.col("_raw_ticket_date").isNotNull() & (F.trim(F.col("_raw_ticket_date")) != "") & F.col("ticket_date").isNull(), "Invalid - Invalid Ticket Date Format")
         .when(F.col("ticket_date").isNull(), "Invalid - Missing Ticket Date")
         .when(F.col("ticket_date") > F.current_date(), "Invalid - Future Ticket Date")
         .when(F.col("_raw_resolution_date").isNotNull() & (F.trim(F.col("_raw_resolution_date")) != "") & F.col("resolution_date").isNull(), "Invalid - Invalid Resolution Date Format")
         .when(F.col("resolution_date").isNotNull() & (F.col("resolution_date") < F.col("ticket_date")), "Invalid - Resolution Before Ticket")
         .when(F.col("_raw_created_date").isNotNull() & (F.trim(F.col("_raw_created_date")) != "") & F.col("created_date").isNull(), "Invalid - Invalid Created Date Format")
         .when(F.col("_raw_updated_date").isNotNull() & (F.trim(F.col("_raw_updated_date")) != "") & F.col("updated_date").isNull(), "Invalid - Invalid Updated Date Format")
         .when(F.col("created_date").isNotNull() & (F.col("created_date") > F.current_date()), "Invalid - Future Created Date")
         .when(F.col("updated_date").isNotNull() & F.col("created_date").isNotNull() & (F.col("updated_date") < F.col("created_date")), "Invalid - Updated Date Before Created Date")
         .when(F.col("priority") == "Unknown", "Warning - Unknown Priority")
         .when(F.col("status") == "Unknown", "Warning - Unknown Status")
         .when(F.col("category") == "Unknown", "Warning - Unknown Category")
         .otherwise("Valid")
    )
)


# ============================================================
# 16. ADD SILVER PROCESSING TIMESTAMP
# ============================================================

df_silver = df_silver.withColumn("_silver_processed_timestamp", F.current_timestamp())


# ============================================================
# 17. SPLIT INVALID AND ACCEPTED RECORDS
# ============================================================

df_invalid = df_silver.filter(F.col("data_quality_status").startswith("Invalid"))

df_accepted = df_silver.filter(~F.col("data_quality_status").startswith("Invalid"))


# ============================================================
# 18. DEDUPLICATE ACCEPTED RECORDS
# ============================================================

window_spec = (
    Window
    .partitionBy("ticket_id")
    .orderBy(
        F.col("updated_date").desc_nulls_last(),
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("ticket_date").desc_nulls_last()
    )
)

df_silver = (
    df_accepted
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)


# ============================================================
# 19. FINAL COLUMN ORDER
# ============================================================

silver_columns = [
    "ticket_id",
    "customer_id",
    "ticket_date",
    "category",
    "priority",
    "status",
    "description",
    "resolution_date",
    "is_open_ticket",
    "resolution_days",
    "ticket_age_days",
    "sla_target_days",
    "sla_breached_flag",
    "ticket_age_band",
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
# 20. DATA QUALITY SUMMARY
# ============================================================

bronze_count = df.count()
accepted_count = df_silver.count()
invalid_count = df_invalid.count()

print("==========================================")
print("SUPPORT TICKETS SILVER LOAD SUMMARY")
print("==========================================")
print(f"Bronze records          : {bronze_count}")
print(f"Silver accepted records : {accepted_count}")
print(f"Silver invalid records  : {invalid_count}")

print("\nAccepted Data Quality Summary:")
df_silver.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nInvalid Record Summary:")
df_invalid.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nStatus Summary:")
df_silver.groupBy("status").count().orderBy("status").show()

print("\nPriority Summary:")
df_silver.groupBy("priority").count().orderBy("priority").show()

print("\nSLA Breach Summary:")
df_silver.groupBy("sla_breached_flag").count().show()


# ============================================================
# 21. WRITE ACCEPTED SILVER TABLE
# ============================================================

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

print(f"Successfully loaded {TARGET_TABLE}")


# ============================================================
# 22. WRITE INVALID SILVER TABLE
# ============================================================

df_invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(INVALID_TABLE)

print(f"Successfully loaded {INVALID_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
