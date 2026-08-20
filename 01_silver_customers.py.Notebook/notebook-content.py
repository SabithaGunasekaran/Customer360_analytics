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

# Customer 360 - Silver Customers Transformation
# Microsoft Fabric Lakehouse / PySpark

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_customers"
TARGET_TABLE = "silver_customers"
INVALID_TABLE = "silver_customers_invalid"

# ============================================================
# 1. READ BRONZE
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze customer records: {df.count()}")


# ============================================================
# 2. PRESERVE RAW DATE VALUES FOR FORMAT VALIDATION
# ============================================================

df_silver = (
    df
    .withColumn("_raw_date_of_birth", F.col("date_of_birth"))
    .withColumn("_raw_created_date", F.col("created_date"))
    .withColumn("_raw_updated_date", F.col("updated_date"))
)


# ============================================================
# 3. CLEAN STRING COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_id", F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
    .withColumn("first_name", F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .withColumn("country", F.initcap(F.trim(F.col("country"))))
)


# ============================================================
# 4. CONVERT DATE COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("date_of_birth", F.to_date(F.col("date_of_birth")))
    .withColumn("created_date", F.to_date(F.col("created_date")))
    .withColumn("updated_date", F.to_date(F.col("updated_date")))
)


# ============================================================
# 5. STANDARDIZE GENDER
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "gender",
        F.when(F.upper(F.trim(F.col("gender"))).isin("M", "MALE"), F.lit("Male"))
         .when(F.upper(F.trim(F.col("gender"))).isin("F", "FEMALE"), F.lit("Female"))
         .when(F.upper(F.trim(F.col("gender"))).isin("OTHER", "O"), F.lit("Other"))
         .otherwise(F.lit("Unknown"))
    )
)


# ============================================================
# 6. STANDARDIZE CUSTOMER STATUS
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "customer_status",
        F.when(F.upper(F.trim(F.col("customer_status"))).isin("ACTIVE", "A"), F.lit("Active"))
         .when(F.upper(F.trim(F.col("customer_status"))).isin("INACTIVE", "I"), F.lit("Inactive"))
         .when(F.upper(F.trim(F.col("customer_status"))).isin("PENDING", "P"), F.lit("Pending"))
         .otherwise(F.lit("Unknown"))
    )
)


# ============================================================
# 7. HANDLE NULL / EMPTY DESCRIPTIVE VALUES
# ============================================================

df_silver = (
    df_silver
    .withColumn("first_name", F.when(F.col("first_name").isNull() | (F.trim(F.col("first_name")) == ""), F.lit("Unknown")).otherwise(F.initcap(F.col("first_name"))))
    .withColumn("last_name", F.when(F.col("last_name").isNull() | (F.trim(F.col("last_name")) == ""), F.lit("Unknown")).otherwise(F.initcap(F.col("last_name"))))
    .withColumn("city", F.when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), F.lit("Unknown")).otherwise(F.col("city")))
    .withColumn("state", F.when(F.col("state").isNull() | (F.trim(F.col("state")) == ""), F.lit("Unknown")).otherwise(F.col("state")))
    .withColumn("country", F.when(F.col("country").isNull() | (F.trim(F.col("country")) == ""), F.lit("Unknown")).otherwise(F.col("country")))
)


# ============================================================
# 8. STANDARDIZE PHONE NUMBER
# ============================================================

df_silver = (
    df_silver
    .withColumn("phone", F.regexp_replace(F.col("phone"), r"[^0-9]", ""))
    .withColumn("phone", F.when(F.col("phone") == "", F.lit(None)).otherwise(F.col("phone")))
)


# ============================================================
# 9. EMAIL DATA QUALITY VALIDATION
# ============================================================

email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

df_silver = (
    df_silver
    .withColumn("email_valid_flag", F.when(F.col("email").isNull() | (F.trim(F.col("email")) == ""), F.lit(False)).when(F.col("email").rlike(email_pattern), F.lit(True)).otherwise(F.lit(False)))
)


# ============================================================
# 10. CREATE FULL NAME
# ============================================================

df_silver = df_silver.withColumn("full_name", F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))))


# ============================================================
# 11. DERIVE CUSTOMER AGE
# ============================================================

df_silver = df_silver.withColumn("age", F.when(F.col("date_of_birth").isNotNull(), F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12).cast("int")))


# ============================================================
# 12. CREATE AGE GROUP
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "age_group",
        F.when(F.col("age").isNull(), "Unknown")
         .when(F.col("age") < 18, "Under 18")
         .when(F.col("age").between(18, 25), "18-25")
         .when(F.col("age").between(26, 35), "26-35")
         .when(F.col("age").between(36, 45), "36-45")
         .when(F.col("age").between(46, 55), "46-55")
         .when(F.col("age").between(56, 65), "56-65")
         .otherwise("66+")
    )
)


# ============================================================
# 13. CUSTOMER TENURE
# ============================================================

df_silver = df_silver.withColumn("customer_tenure_years", F.when(F.col("created_date").isNotNull(), F.round(F.months_between(F.current_date(), F.col("created_date")) / 12, 1)))


# ============================================================
# 14. ACTIVE CUSTOMER FLAG
# ============================================================

df_silver = df_silver.withColumn("is_active_customer", F.when(F.col("customer_status") == "Active", F.lit(True)).otherwise(F.lit(False)))


# ============================================================
# 15. DATA QUALITY VALIDATION
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "data_quality_status",
        F.when(F.col("customer_id").isNull(), F.lit("Invalid - Missing Customer ID"))
         .when(F.col("_raw_date_of_birth").isNotNull() & (F.trim(F.col("_raw_date_of_birth")) != "") & F.col("date_of_birth").isNull(), F.lit("Invalid - Invalid DOB Format"))
         .when(F.col("date_of_birth").isNotNull() & (F.col("date_of_birth") > F.current_date()), F.lit("Invalid - Future DOB"))
         .when(F.col("_raw_created_date").isNotNull() & (F.trim(F.col("_raw_created_date")) != "") & F.col("created_date").isNull(), F.lit("Invalid - Invalid Created Date Format"))
         .when(F.col("_raw_updated_date").isNotNull() & (F.trim(F.col("_raw_updated_date")) != "") & F.col("updated_date").isNull(), F.lit("Invalid - Invalid Updated Date Format"))
         .when(F.col("created_date").isNotNull() & (F.col("created_date") > F.current_date()), F.lit("Invalid - Future Created Date"))
         .when(F.col("updated_date").isNotNull() & F.col("created_date").isNotNull() & (F.col("updated_date") < F.col("created_date")), F.lit("Invalid - Updated Date Before Created Date"))
         .when(~F.col("email_valid_flag"), F.lit("Warning - Invalid or Missing Email"))
         .otherwise(F.lit("Valid"))
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
# 18. DEDUPLICATION
# ============================================================

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        F.col("updated_date").desc_nulls_last(),
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("created_date").desc_nulls_last()
    )
)

df_silver = (
    df_accepted
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)


# ============================================================
# 19. FINAL SILVER COLUMN ORDER
# ============================================================

df_silver = df_silver.select(
    "customer_id",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "email_valid_flag",
    "phone",
    "gender",
    "date_of_birth",
    "age",
    "age_group",
    "city",
    "state",
    "country",
    "customer_status",
    "is_active_customer",
    "created_date",
    "updated_date",
    "customer_tenure_years",
    "data_quality_status",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)


# ============================================================
# 20. FINAL INVALID TABLE COLUMN ORDER
# ============================================================

df_invalid = df_invalid.select(
    "customer_id",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "email_valid_flag",
    "phone",
    "gender",
    "date_of_birth",
    "age",
    "age_group",
    "city",
    "state",
    "country",
    "customer_status",
    "is_active_customer",
    "created_date",
    "updated_date",
    "customer_tenure_years",
    "data_quality_status",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)


# ============================================================
# 21. DATA QUALITY SUMMARY
# ============================================================

bronze_count = df.count()
accepted_count = df_silver.count()
invalid_count = df_invalid.count()

print("==========================================")
print("CUSTOMER SILVER LOAD SUMMARY")
print("==========================================")
print(f"Bronze records          : {bronze_count}")
print(f"Silver accepted records : {accepted_count}")
print(f"Silver invalid records  : {invalid_count}")

print("\nData Quality Summary:")
df_silver.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nInvalid Record Summary:")
df_invalid.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nCustomer Status Summary:")
df_silver.groupBy("customer_status").count().show()

print("\nGender Summary:")
df_silver.groupBy("gender").count().show()


# ============================================================
# 22. WRITE ACCEPTED SILVER TABLE
# ============================================================

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

print(f"Successfully loaded {TARGET_TABLE}")


# ============================================================
# 23. WRITE INVALID SILVER TABLE
# ============================================================

df_invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(INVALID_TABLE)

print(f"Successfully loaded {INVALID_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
