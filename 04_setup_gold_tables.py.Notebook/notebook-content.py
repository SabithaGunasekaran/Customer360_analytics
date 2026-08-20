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
# CUSTOMER 360 - GOLD TABLE SETUP
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_customer_360 (
    
    master_customer_id STRING,
    customer_id STRING,

    customer_name STRING,
    email STRING,
    phone STRING,
    country STRING,
    customer_status STRING,

    -- Account metrics
    total_balance DECIMAL(18,2),
    account_count INT,
    active_account_count INT,

    -- Support metrics
    ticket_count INT,
    open_ticket_count INT,
    high_priority_ticket_count INT,

    -- Marketing metrics
    marketing_interaction_count INT,
    click_count INT,
    conversion_count INT,

    -- Customer analytics
    engagement_score INT,
    support_risk_score INT,

    customer_segment STRING,

    _updated_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: gold_customer_360")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
