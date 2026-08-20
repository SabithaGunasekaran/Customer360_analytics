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

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC DELETE FROM silver_customers;
# MAGIC DELETE FROM silver_accounts;
# MAGIC DELETE FROM silver_support_tickets;
# MAGIC DELETE FROM silver_marketing_interactions;
# MAGIC 
# MAGIC DELETE FROM bronze_customers;
# MAGIC DELETE FROM bronze_accounts;
# MAGIC DELETE FROM bronze_support_tickets;
# MAGIC DELETE FROM bronze_marketing_interactions;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
