import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate(df, key_column):
    window_spec = Window.partitionBy(key_column).orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("source_file").desc_nulls_last(),
        # Stable content order resolves different rows sharing timestamp and source.
        F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])).desc(),
    )

    return (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_ROOT_PATH",
        "BRONZE_ROOT_PATH",
        "SILVER_ROOT_PATH",
        "REJECT_ROOT_PATH",
        "BATCH_DATE",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

batch_date = args["BATCH_DATE"]

raw_root_path = args["RAW_ROOT_PATH"]
bronze_root_path = args["BRONZE_ROOT_PATH"]
silver_root_path = args["SILVER_ROOT_PATH"]
reject_root_path = args["REJECT_ROOT_PATH"]
bronze_orders_path = f"{bronze_root_path}/orders"
bronze_customers_path = f"{bronze_root_path}/customers"
silver_orders_path = f"{silver_root_path}/orders/batch_date={batch_date}"
silver_customers_path = f"{silver_root_path}/customers/batch_date={batch_date}"
silver_products_path = f"{silver_root_path}/products/batch_date={batch_date}"
silver_stores_path = f"{silver_root_path}/stores/batch_date={batch_date}"
silver_order_items_path = f"{silver_root_path}/order_items/batch_date={batch_date}"
silver_payments_path = f"{silver_root_path}/payments/batch_date={batch_date}"
reject_customers_path = f"{reject_root_path}/customers/batch_date={batch_date}"
reject_orders_path = f"{reject_root_path}/orders/batch_date={batch_date}"
reject_order_items_path = f"{reject_root_path}/order_items/batch_date={batch_date}"
fx_rates_path = f"{raw_root_path}/currency/batch_date={batch_date}/fx_rates.csv"

print(f"Batch Date            : {batch_date}")
print(f"Bronze Orders Path    : {bronze_orders_path}")
print(f"Bronze Customers Path : {bronze_customers_path}")
print(f"Silver Orders Path    : {silver_orders_path}")
print(f"Silver Customers Path : {silver_customers_path}")
print(f"Reject Customers Path : {reject_customers_path}")
print(f"FX Rates Path         : {fx_rates_path}")


# Read Bronze Orders
orders_df = spark.read.parquet(bronze_orders_path)

print("Orders Schema")
orders_df.printSchema()

print("Sample Orders")
orders_df.show(5, truncate=False)


# Read FX Rates
fx_df = spark.read.option("header", True).csv(fx_rates_path)
fx_df = fx_df.withColumn("exchange_rate", F.col("exchange_rate").cast("double")).select(
    "target_currency", "exchange_rate"
)

print("FX Schema")
fx_df.printSchema()

print("Sample FX Rates")
fx_df.show(10, truncate=False)

# Read Bronze Customers
customers_df = spark.read.parquet(bronze_customers_path)

print("Customers Schema")
customers_df.printSchema()

print("Sample Customers")
customers_df.show(5, truncate=False)

# Deduplicate Customers
customers_df = deduplicate(customers_df, "customer_id")

# Read Bronze Products
products_df = spark.read.parquet(f"{bronze_root_path}/products/")

print("Products Schema")
products_df.printSchema()

print("Sample Products")
products_df.show(5, truncate=False)

# Read Bronze Stores
stores_df = spark.read.parquet(f"{bronze_root_path}/stores/")

print("Stores Schema")
stores_df.printSchema()

print("Sample Stores")
stores_df.show(5, truncate=False)

# Read Bronze Order Items
order_items_df = spark.read.parquet(f"{bronze_root_path}/order_items/")

print("Order Items Schema")
order_items_df.printSchema()

print("Sample Order Items")
order_items_df.show(5, truncate=False)

# Read Bronze Payments
payments_df = spark.read.parquet(f"{bronze_root_path}/payments/")

print("Payments Schema")
payments_df.printSchema()

print("Sample Payments")
payments_df.show(5, truncate=False)

# Bronze row count
bronze_count = orders_df.count()
print(f"Bronze Row Count : {bronze_count}")

# Deduplicate records using the same deterministic order as other entities.
orders_df = deduplicate(orders_df, "order_id")

silver_count = orders_df.count()

print(f"Silver Row Count : {silver_count}")
print(f"Duplicate Rows Removed : {bronze_count - silver_count}")


# Mandatory columns
mandatory_columns = [
    "order_id",
    "customer_id",
    "store_id",
    "order_date",
    "order_status",
    "currency_code",
    "created_at",
    "ingestion_timestamp",
    "batch_date",
    "source_system",
    "source_file",
]

# Null validation
print("Null Counts")

null_counts = orders_df.select(
    [
        F.count(F.when(F.col(column).isNull(), column)).alias(column)
        for column in mandatory_columns
    ]
)

null_counts.show(truncate=False)


# Remove rows with mandatory nulls
missing_required = F.lit(False)
for column in mandatory_columns:
    missing_required = missing_required | F.col(column).isNull()
reject_orders_df = orders_df.filter(missing_required).withColumn(
    "reject_reason", F.lit("Null Mandatory Order Field")
)
orders_df = orders_df.filter(~missing_required)

final_count = orders_df.count()

print(f"Final Silver Row Count : {final_count}")
print(f"Rows Removed Due To Nulls : {silver_count - final_count}")


# Join FX Rates
orders_df = orders_df.join(
    fx_df, orders_df.currency_code == fx_df.target_currency, "left"
).drop("target_currency")

print("Orders With Exchange Rates")
orders_df.select("order_id", "currency_code", "exchange_rate").show(20, truncate=False)


# Customer Cleansing
customers_df = customers_df.withColumn(
    "first_name", F.initcap(F.trim(F.col("first_name")))
).withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))

customers_df = customers_df.withColumn(
    "state",
    F.when(
        F.lower(F.regexp_replace(F.col("state"), " ", "")) == "tn", "Tamil Nadu"
    )
    .when(
        F.lower(F.regexp_replace(F.col("state"), " ", "")) == "tamilnadu", "Tamil Nadu"
    )
    .when(F.lower(F.regexp_replace(F.col("state"), " ", "")) == "ka", "Karnataka")
    .when(
        F.lower(F.regexp_replace(F.col("state"), " ", "")) == "karnataka", "Karnataka"
    )
    .when(F.lower(F.regexp_replace(F.col("state"), " ", "")) == "mh", "Maharashtra")
    .when(
        F.lower(F.regexp_replace(F.col("state"), " ", "")) == "maharashtra",
        "Maharashtra",
    )
    .otherwise(F.initcap(F.trim(F.col("state")))),
)

customers_df = customers_df.withColumn(
    "country", F.initcap(F.trim(F.col("country")))
).withColumn(
    "phone", F.regexp_replace(F.col("phone"), r"[\s\-\(\)\+]", "")
)

email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

valid_customers_df = customers_df.filter(F.col("email").rlike(email_pattern))
reject_customers_df = customers_df.filter(
    F.col("email").isNull() | ~F.col("email").rlike(email_pattern)
).withColumn(
    "reject_reason",
    F.when(F.col("email").isNull(), F.lit("Null Email")).otherwise(F.lit("Invalid Email"))
)

print(f"Valid Customers    : {valid_customers_df.count()}")
print(f"Rejected Customers : {reject_customers_df.count()}")

print("Valid Customers")

valid_customers_df.show(10, truncate=False)

print("Rejected Customers")

reject_customers_df.show(10, truncate=False)

# Product Cleansing
products_df = deduplicate(products_df, "product_id")

products_df = products_df.withColumn(
    "product_name", F.initcap(F.trim(F.col("product_name")))
).withColumn("category", F.initcap(F.trim(F.col("category"))))

print(f"Silver Products : {products_df.count()}")

products_df.show(10, truncate=False)

# Store Cleansing
stores_df = deduplicate(stores_df, "store_id")

stores_df = stores_df.withColumn(
    "store_name", F.initcap(F.trim(F.col("store_name")))
).withColumn(
    "region", F.initcap(F.trim(F.col("region")))
).withColumn(
    "country", F.initcap(F.trim(F.col("country")))
)

print(f"Silver Stores : {stores_df.count()}")

stores_df.show(10, truncate=False)

# Order Items Cleansing
order_items_df = deduplicate(order_items_df, "order_item_id")
# Known rejected parents cascade to quarantine. Unknown parents remain visible to dbt tests.
rejected_order_ids = reject_orders_df.select("order_id").filter(
    F.col("order_id").isNotNull()
).distinct()
reject_order_items_df = order_items_df.join(
    rejected_order_ids, "order_id", "left_semi"
).withColumn("reject_reason", F.lit("Rejected Parent Order"))
order_items_df = order_items_df.join(rejected_order_ids, "order_id", "left_anti")
print(f"Silver Order Items : {order_items_df.count()}")

order_items_df.show(10, truncate=False)

# Payments Cleansing
payments_df = deduplicate(payments_df, "payment_id")

payments_df = payments_df.withColumn(
    "payment_method",
    F.initcap(F.regexp_replace(F.trim(F.col("payment_method")), "_", " ")),
)

print(f"Silver Payments : {payments_df.count()}")

payments_df.show(10, truncate=False)


# Write Silver layer
orders_df.write.mode("overwrite").parquet(silver_orders_path)

print("Silver Orders written successfully.")

products_df.write.mode("overwrite").parquet(silver_products_path)

print("Silver Products written successfully.")

stores_df.write.mode("overwrite").parquet(silver_stores_path)

print("Silver Stores written successfully.")

order_items_df.write.mode("overwrite").parquet(silver_order_items_path)

print("Silver Order Items written successfully.")

payments_df.write.mode("overwrite").parquet(silver_payments_path)

print("Silver Payments written successfully.")

valid_customers_df.write.mode("overwrite").parquet(silver_customers_path)

print("Silver Customers written successfully.")

reject_customers_df.write.mode("overwrite").parquet(reject_customers_path)
reject_orders_df.write.mode("overwrite").parquet(reject_orders_path)
reject_order_items_df.write.mode("overwrite").parquet(reject_order_items_path)

print("Rejected Customers written successfully.")

job.commit()
