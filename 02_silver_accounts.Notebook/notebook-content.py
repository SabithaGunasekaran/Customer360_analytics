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

# Customer 360 - Silver Accounts Transformation
# Microsoft Fabric Lakehouse / PySpark

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_accounts"
TARGET_TABLE = "silver_accounts"
INVALID_TABLE = "silver_accounts_invalid"


# ============================================================
# 1. READ BRONZE
# ============================================================

df = spark.table(SOURCE_TABLE)

print(f"Bronze account records: {df.count()}")


# ============================================================
# 2. PRESERVE RAW VALUES FOR DATA QUALITY VALIDATION
# ============================================================

df_silver = (
    df
    .withColumn("_raw_balance", F.col("balance"))
    .withColumn("_raw_opened_date", F.col("opened_date"))
    .withColumn("_raw_closed_date", F.col("closed_date"))
    .withColumn("_raw_created_date", F.col("created_date"))
    .withColumn("_raw_updated_date", F.col("updated_date"))
)


# ============================================================
# 3. CLEAN BUSINESS KEYS
# ============================================================

df_silver = (
    df_silver
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("account_id", F.when(F.col("account_id") == "", F.lit(None)).otherwise(F.col("account_id")))
    .withColumn("customer_id", F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
)


# ============================================================
# 4. CLEAN ACCOUNT / PRODUCT COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("account_type", F.initcap(F.trim(F.col("account_type"))))
    .withColumn("account_category", F.initcap(F.trim(F.col("account_category"))))
    .withColumn("product_family", F.initcap(F.trim(F.col("product_family"))))
    .withColumn("deposit_credit_type", F.initcap(F.trim(F.col("deposit_credit_type"))))
    .withColumn("risk_category", F.initcap(F.trim(F.col("risk_category"))))
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
    .withColumn("account_status", F.initcap(F.trim(F.col("account_status"))))
)


# ============================================================
# 5. CLEAN BRANCH COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("branch_code", F.upper(F.trim(F.col("branch_code"))))
    .withColumn("branch_name", F.initcap(F.trim(F.col("branch_name"))))
    .withColumn("branch_city", F.initcap(F.trim(F.col("branch_city"))))
    .withColumn("branch_state", F.initcap(F.trim(F.col("branch_state"))))
    .withColumn("branch_country", F.initcap(F.trim(F.col("branch_country"))))
    .withColumn("branch_region", F.initcap(F.trim(F.col("branch_region"))))
    .withColumn("branch_type", F.initcap(F.trim(F.col("branch_type"))))
)


# ============================================================
# 6. STANDARDIZE ACCOUNT TYPE
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "account_type",
        F.when(F.upper(F.col("account_type")).isin("SAVINGS", "SAVING"), "Savings")
         .when(F.upper(F.col("account_type")).isin("CHECKING", "CHEQUING"), "Checking")
         .when(F.upper(F.col("account_type")).isin("CREDIT", "CREDIT CARD"), "Credit Card")
         .when(F.upper(F.col("account_type")).isin("LINE OF CREDIT", "LOC"), "Line Of Credit")
         .otherwise("Other")
    )
)


# ============================================================
# 7. STANDARDIZE ACCOUNT STATUS
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "account_status",
        F.when(F.upper(F.col("account_status")).isin("ACTIVE", "A"), "Active")
         .when(F.upper(F.col("account_status")).isin("CLOSED", "C"), "Closed")
         .when(F.upper(F.col("account_status")).isin("SUSPENDED", "S"), "Suspended")
         .otherwise("Unknown")
    )
)


# ============================================================
# 8. STANDARDIZE BOOLEAN FLAGS
# ============================================================

df_silver = (
    df_silver
    .withColumn("interest_bearing_flag", F.when(F.upper(F.trim(F.col("interest_bearing_flag"))).isin("1", "Y", "YES", "TRUE"), True).otherwise(False))
    .withColumn("is_active_product", F.when(F.upper(F.trim(F.col("is_active_product"))).isin("1", "Y", "YES", "TRUE"), True).otherwise(False))
    .withColumn("is_active_branch", F.when(F.upper(F.trim(F.col("is_active_branch"))).isin("1", "Y", "YES", "TRUE"), True).otherwise(False))
)


# ============================================================
# 9. CAST BALANCE AND DATE COLUMNS
# ============================================================

df_silver = (
    df_silver
    .withColumn("balance", F.col("balance").cast("decimal(18,2)"))
    .withColumn("opened_date", F.to_date(F.col("opened_date")))
    .withColumn("closed_date", F.to_date(F.col("closed_date")))
    .withColumn("created_date", F.to_date(F.col("created_date")))
    .withColumn("updated_date", F.to_date(F.col("updated_date")))
)


# ============================================================
# 10. HANDLE NULL / EMPTY DESCRIPTIVE VALUES
# ============================================================

df_silver = (
    df_silver
    .withColumn("account_category", F.when(F.col("account_category").isNull() | (F.trim(F.col("account_category")) == ""), "Unknown").otherwise(F.col("account_category")))
    .withColumn("product_family", F.when(F.col("product_family").isNull() | (F.trim(F.col("product_family")) == ""), "Unknown").otherwise(F.col("product_family")))
    .withColumn("deposit_credit_type", F.when(F.col("deposit_credit_type").isNull() | (F.trim(F.col("deposit_credit_type")) == ""), "Unknown").otherwise(F.col("deposit_credit_type")))
    .withColumn("risk_category", F.when(F.col("risk_category").isNull() | (F.trim(F.col("risk_category")) == ""), "Unknown").otherwise(F.col("risk_category")))
    .withColumn("currency_code", F.when(F.col("currency_code").isNull() | (F.trim(F.col("currency_code")) == ""), "Unknown").otherwise(F.col("currency_code")))
    .withColumn("branch_code", F.when(F.col("branch_code").isNull() | (F.trim(F.col("branch_code")) == ""), "Unknown").otherwise(F.col("branch_code")))
    .withColumn("branch_name", F.when(F.col("branch_name").isNull() | (F.trim(F.col("branch_name")) == ""), "Unknown").otherwise(F.col("branch_name")))
    .withColumn("branch_city", F.when(F.col("branch_city").isNull() | (F.trim(F.col("branch_city")) == ""), "Unknown").otherwise(F.col("branch_city")))
    .withColumn("branch_state", F.when(F.col("branch_state").isNull() | (F.trim(F.col("branch_state")) == ""), "Unknown").otherwise(F.col("branch_state")))
    .withColumn("branch_country", F.when(F.col("branch_country").isNull() | (F.trim(F.col("branch_country")) == ""), "Unknown").otherwise(F.col("branch_country")))
    .withColumn("branch_region", F.when(F.col("branch_region").isNull() | (F.trim(F.col("branch_region")) == ""), "Unknown").otherwise(F.col("branch_region")))
    .withColumn("branch_type", F.when(F.col("branch_type").isNull() | (F.trim(F.col("branch_type")) == ""), "Unknown").otherwise(F.col("branch_type")))
)


# ============================================================
# 11. ACTIVE ACCOUNT FLAG
# ============================================================

df_silver = df_silver.withColumn("is_active_account", F.when(F.col("account_status") == "Active", True).otherwise(False))


# ============================================================
# 12. ACCOUNT AGE
# ============================================================
# For closed accounts, calculate age up to closed date.
# For open accounts, calculate age up to current date.

df_silver = df_silver.withColumn("account_age_years", F.when(F.col("opened_date").isNotNull(), F.round(F.months_between(F.coalesce(F.col("closed_date"), F.current_date()), F.col("opened_date")) / 12, 1)))


# ============================================================
# 13. BALANCE BAND
# ============================================================

df_silver = (
    df_silver
    .withColumn(
        "balance_band",
        F.when(F.col("balance").isNull(), "Unknown")
         .when(F.col("balance") < 0, "Negative")
         .when(F.col("balance") < 1000, "Under 1K")
         .when(F.col("balance") < 10000, "1K-10K")
         .when(F.col("balance") < 50000, "10K-50K")
         .otherwise("50K+")
    )
)


# ============================================================
# 14. DATA QUALITY VALIDATION
# ============================================================
# Critical failures are checked before warnings.

df_silver = (
    df_silver
    .withColumn(
        "data_quality_status",
        F.when(F.col("account_id").isNull(), "Invalid - Missing Account ID")
         .when(F.col("customer_id").isNull(), "Invalid - Missing Customer ID")
         .when(F.col("_raw_opened_date").isNotNull() & (F.trim(F.col("_raw_opened_date")) != "") & F.col("opened_date").isNull(), "Invalid - Invalid Opened Date Format")
         .when(F.col("_raw_closed_date").isNotNull() & (F.trim(F.col("_raw_closed_date")) != "") & F.col("closed_date").isNull(), "Invalid - Invalid Closed Date Format")
         .when(F.col("_raw_created_date").isNotNull() & (F.trim(F.col("_raw_created_date")) != "") & F.col("created_date").isNull(), "Invalid - Invalid Created Date Format")
         .when(F.col("_raw_updated_date").isNotNull() & (F.trim(F.col("_raw_updated_date")) != "") & F.col("updated_date").isNull(), "Invalid - Invalid Updated Date Format")
         .when(F.col("opened_date").isNotNull() & (F.col("opened_date") > F.current_date()), "Invalid - Future Open Date")
         .when(F.col("closed_date").isNotNull() & F.col("opened_date").isNotNull() & (F.col("closed_date") < F.col("opened_date")), "Invalid - Closed Before Opened")
         .when(F.col("created_date").isNotNull() & (F.col("created_date") > F.current_date()), "Invalid - Future Created Date")
         .when(F.col("updated_date").isNotNull() & F.col("created_date").isNotNull() & (F.col("updated_date") < F.col("created_date")), "Invalid - Updated Date Before Created Date")
         .when(F.col("_raw_balance").isNotNull() & (F.trim(F.col("_raw_balance")) != "") & F.col("balance").isNull(), "Invalid - Invalid Balance Format")
         .when(F.col("balance").isNull(), "Warning - Missing Balance")
         .when(F.col("account_type") == "Other", "Warning - Unknown Account Type")
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
# If equal, latest ingestion timestamp wins.

window_spec = (
    Window
    .partitionBy("account_id")
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
# 18. FINAL SILVER COLUMN ORDER
# ============================================================

silver_columns = [
    "account_id",
    "customer_id",
    "account_type",
    "account_category",
    "product_family",
    "deposit_credit_type",
    "interest_bearing_flag",
    "risk_category",
    "currency_code",
    "is_active_product",
    "account_status",
    "balance",
    "balance_band",
    "opened_date",
    "closed_date",
    "account_age_years",
    "is_active_account",
    "branch_code",
    "branch_name",
    "branch_city",
    "branch_state",
    "branch_country",
    "branch_region",
    "branch_type",
    "is_active_branch",
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
print("ACCOUNT SILVER LOAD SUMMARY")
print("==========================================")
print(f"Bronze records          : {bronze_count}")
print(f"Silver accepted records : {accepted_count}")
print(f"Silver invalid records  : {invalid_count}")

print("\nAccepted Data Quality Summary:")
df_silver.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nInvalid Record Summary:")
df_invalid.groupBy("data_quality_status").count().orderBy("data_quality_status").show(truncate=False)

print("\nAccount Status Summary:")
df_silver.groupBy("account_status").count().show()

print("\nAccount Type Summary:")
df_silver.groupBy("account_type").count().show()

print("\nBalance Band Summary:")
df_silver.groupBy("balance_band").count().show()


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
