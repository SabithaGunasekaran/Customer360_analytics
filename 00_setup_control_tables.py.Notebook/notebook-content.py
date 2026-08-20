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

# ============================================================
# CUSTOMER 360 - ONE-TIME CONTROL TABLE SETUP
# ============================================================

# ------------------------------------------------------------
# 1. FILE PROCESSING CONTROL
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS ctl_file_processing (
    file_id STRING,
    file_name STRING,
    file_path STRING,
    source_system STRING,
    file_size_bytes BIGINT,
    file_modified_timestamp TIMESTAMP,
    processing_status STRING,
    processed_timestamp TIMESTAMP,
    pipeline_run_id STRING,
    row_count BIGINT,
    error_message STRING
)
USING DELTA
""")

print("Created/verified: ctl_file_processing")


# ------------------------------------------------------------
# 2. PIPELINE RUN AUDIT
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS audit_pipeline_run (
    pipeline_run_id STRING,
    pipeline_name STRING,
    pipeline_start_timestamp TIMESTAMP,
    pipeline_end_timestamp TIMESTAMP,
    pipeline_status STRING,
    records_processed BIGINT,
    error_message STRING
)
USING DELTA
""")

print("Created/verified: audit_pipeline_run")


# ------------------------------------------------------------
# 3. DATA QUALITY AUDIT
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS audit_data_quality (
    pipeline_run_id STRING,
    table_name STRING,
    check_name STRING,
    records_checked BIGINT,
    failed_records BIGINT,
    check_status STRING,
    check_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: audit_data_quality")


# ------------------------------------------------------------
# 4. PIPELINE CONFIGURATION
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS ctl_pipeline_config (
    pipeline_name STRING,
    source_system STRING,
    source_path STRING,
    target_table STRING,
    is_enabled BOOLEAN,
    load_type STRING
)
USING DELTA
""")

print("Created/verified: ctl_pipeline_config")


# ------------------------------------------------------------
# VERIFY TABLES
# ------------------------------------------------------------

print("\nCustomer 360 control tables:")
spark.sql("""
SHOW TABLES
""").show(truncate=False)

print("\nControl table setup completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
