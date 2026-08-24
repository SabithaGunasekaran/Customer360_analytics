# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# “We start by reading the raw customer data from the Bronze Delta table.”


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "bronze_customers"
TARGET_TABLE = "silver_customers"

df = spark.table(SOURCE_TABLE)

print(f"Bronze customer records: {df.count()}")

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# “First, we clean and standardize the descriptive customer fields.”


# CELL ********************

df_silver = (
    df
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("province", F.upper(F.trim(F.col("province"))))
    .withColumn("preferred_cuisine", F.initcap(F.trim(F.col("preferred_cuisine"))))
)

display(
    df_silver.select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "province",
        "preferred_cuisine"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# “Next, we standardize postal codes by removing spaces and converting them to uppercase.”

# CELL ********************

df_silver = (
    df_silver
    .withColumn(
        "postal_code",
        F.upper(
            F.regexp_replace(
                F.trim(F.col("postal_code")),
                r"\s+",
                ""
            )
        )
    )
)

display(
    df_silver.select(
        "customer_id",
        "postal_code"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# “Bronze keeps the source date as a string. In Silver, we convert it to a proper date type.”


# CELL ********************

df_silver = (
    df_silver
    .withColumn(
        "signup_date",
        F.to_date(F.col("signup_date"))
    )
)

display(
    df_silver.select(
        "customer_id",
        "signup_date"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# “We derive the signup year as a simple reporting attribute.”

# CELL ********************

df_silver = (
    df_silver
    .withColumn(
        "signup_year",
        F.year(F.col("signup_date"))
    )
)

display(
    df_silver.select(
        "customer_id",
        "signup_date",
        "signup_year"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# “The source can contain different representations of Yes and No. We convert them into a Boolean value.”


# CELL ********************

df_silver = (
    df_silver
    .withColumn(
        "loyalty_member",
        F.when(
            F.upper(F.trim(F.col("loyalty_member")))
            .isin("Y", "YES", "TRUE", "1"),
            True
        )
        .otherwise(False)
    )
)

display(
    df_silver.select(
        "customer_id",
        "loyalty_member"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = (
    df_silver
    .withColumn(
        "marketing_opt_in",
        F.when(
            F.upper(F.trim(F.col("marketing_opt_in")))
            .isin("Y", "YES", "TRUE", "1"),
            True
        )
        .otherwise(False)
    )
)

display(
    df_silver.select(
        "customer_id",
        "loyalty_member",
        "marketing_opt_in"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

before_count = df_silver.count()

df_silver = (
    df_silver
    .filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    )
)

after_count = df_silver.count()

print(f"Records before validation : {before_count}")
print(f"Records after validation  : {after_count}")
print(f"Invalid records removed   : {before_count - after_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last()
    )
)

df_silver = (
    df_silver
    .withColumn(
        "_row_num",
        F.row_number().over(window_spec)
    )
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"Records after deduplication: {df_silver.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = (
    df_silver
    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = df_silver.select(
    "customer_id",
    "customer_name",
    "email",
    "phone",
    "city",
    "province",
    "postal_code",
    "signup_date",
    "signup_year",
    "preferred_cuisine",
    "loyalty_member",
    "marketing_opt_in",
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_silver_processed_timestamp"
)

display(df_silver.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully loaded {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
