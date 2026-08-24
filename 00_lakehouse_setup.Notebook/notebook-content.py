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

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_customers
# MAGIC (
# MAGIC     -- Source columns
# MAGIC     customer_id         STRING,
# MAGIC     customer_name       STRING,
# MAGIC     email               STRING,
# MAGIC     phone               STRING,
# MAGIC     city                STRING,
# MAGIC     province            STRING,
# MAGIC     postal_code         STRING,
# MAGIC     signup_date         STRING,
# MAGIC     preferred_cuisine   STRING,
# MAGIC     loyalty_member      STRING,
# MAGIC     marketing_opt_in    STRING,
# MAGIC 
# MAGIC     -- Ingestion metadata
# MAGIC     _source_file         STRING,
# MAGIC     _source_system       STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_restaurants
# MAGIC (
# MAGIC     restaurant_id STRING,
# MAGIC     restaurant_name STRING,
# MAGIC     cuisine_type STRING,
# MAGIC     city STRING,
# MAGIC     restaurant_rating STRING,
# MAGIC 
# MAGIC     _source_file STRING,
# MAGIC     _source_system STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_orders
# MAGIC (
# MAGIC     order_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     restaurant_id STRING,
# MAGIC     order_date STRING,
# MAGIC     quantity STRING,
# MAGIC     unit_price STRING,
# MAGIC     order_status STRING,
# MAGIC     payment_method STRING,
# MAGIC 
# MAGIC     _source_file STRING,
# MAGIC     _source_system STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze_deliveries
# MAGIC (
# MAGIC     delivery_id STRING,
# MAGIC     order_id STRING,
# MAGIC     delivery_status STRING,
# MAGIC     delivery_start_time STRING,
# MAGIC     delivery_end_time STRING,
# MAGIC     driver_rating STRING,
# MAGIC 
# MAGIC     _source_file STRING,
# MAGIC     _source_system STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_customers
# MAGIC (
# MAGIC     customer_id                  STRING,
# MAGIC     customer_name                STRING,
# MAGIC     email                        STRING,
# MAGIC     phone                        STRING,
# MAGIC 
# MAGIC     city                         STRING,
# MAGIC     province                     STRING,
# MAGIC     postal_code                  STRING,
# MAGIC 
# MAGIC     signup_date                  DATE,
# MAGIC     signup_year                  INT,
# MAGIC 
# MAGIC     preferred_cuisine            STRING,
# MAGIC     loyalty_member               BOOLEAN,
# MAGIC     marketing_opt_in             BOOLEAN,
# MAGIC 
# MAGIC     _source_file                 STRING,
# MAGIC     _source_system               STRING,
# MAGIC     _ingestion_timestamp         TIMESTAMP,
# MAGIC     _silver_processed_timestamp  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_restaurants
# MAGIC (
# MAGIC     restaurant_id STRING,
# MAGIC     restaurant_name STRING,
# MAGIC     cuisine_type STRING,
# MAGIC     city STRING,
# MAGIC     restaurant_rating DECIMAL(3,1),
# MAGIC 
# MAGIC     _source_file STRING,
# MAGIC     _source_system STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP,
# MAGIC     _silver_processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_orders
# MAGIC (
# MAGIC     order_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     restaurant_id STRING,
# MAGIC 
# MAGIC     order_date DATE,
# MAGIC 
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(10,2),
# MAGIC     order_amount DECIMAL(12,2),
# MAGIC 
# MAGIC     order_status STRING,
# MAGIC     payment_method STRING,
# MAGIC 
# MAGIC     _source_file STRING,
# MAGIC     _source_system STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP,
# MAGIC     _silver_processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver_deliveries
# MAGIC (
# MAGIC     delivery_id STRING,
# MAGIC     order_id STRING,
# MAGIC 
# MAGIC     delivery_status STRING,
# MAGIC 
# MAGIC     delivery_start_time TIMESTAMP,
# MAGIC     delivery_end_time TIMESTAMP,
# MAGIC     delivery_minutes INT,
# MAGIC 
# MAGIC     driver_rating DECIMAL(3,1),
# MAGIC 
# MAGIC     _source_file STRING,
# MAGIC     _source_system STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP,
# MAGIC     _silver_processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
