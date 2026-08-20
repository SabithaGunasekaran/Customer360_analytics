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
# CUSTOMER 360 - SILVER TABLE SETUP
# Run once
# ============================================================


# ------------------------------------------------------------
# 1. SILVER CUSTOMERS
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country STRING,
    signup_date DATE,
    customer_status STRING,

    _source_file STRING,
    _updated_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: silver_customers")


# ------------------------------------------------------------
# 2. SILVER ACCOUNTS
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    balance DECIMAL(18,2),
    open_date DATE,

    _source_file STRING,
    _updated_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: silver_accounts")


# ------------------------------------------------------------
# 3. SILVER SUPPORT TICKETS
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_support_tickets (
    ticket_id STRING,
    customer_id STRING,
    created_date DATE,
    category STRING,
    priority STRING,
    status STRING,
    resolution_days INT,

    _source_file STRING,
    _updated_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: silver_support_tickets")


# ------------------------------------------------------------
# 4. SILVER MARKETING INTERACTIONS
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_marketing_interactions (
    interaction_id STRING,
    customer_id STRING,
    campaign STRING,
    channel STRING,
    interaction_date DATE,
    interaction_type STRING,

    _source_file STRING,
    _updated_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: silver_marketing_interactions")


# ------------------------------------------------------------
# VERIFY
# ------------------------------------------------------------

spark.sql("""
SHOW TABLES
""").show(truncate=False)

print("Silver table setup completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
