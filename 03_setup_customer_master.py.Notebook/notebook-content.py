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
# CUSTOMER 360 - CUSTOMER MASTER SETUP
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_customer_master (
    master_customer_id STRING,
    customer_id STRING,

    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country STRING,

    signup_date DATE,
    customer_status STRING,

    _updated_timestamp TIMESTAMP
)
USING DELTA
""")

print("Created/verified: silver_customer_master")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
