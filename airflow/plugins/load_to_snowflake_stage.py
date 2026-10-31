import os
import shutil
import tempfile

import boto3
import snowflake.connector

RAW_BUCKET_NAME = os.environ["RAW_BUCKET_NAME"]

STAGE_NAME = os.environ["SNOWFLAKE_STAGE"]

DATASETS = [
    "customers",
    "products",
    "stores",
    "orders",
    "order_items",
    "payments",
]

SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]


def get_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def download_silver_files(batch_date):
    s3 = boto3.client("s3")
    temp_dir = tempfile.mkdtemp()
    local_files = []

    for dataset in DATASETS:
        prefix = f"archive/l2/{dataset}/batch_date={batch_date}/"
        dataset_dir = os.path.join(temp_dir, dataset)
        os.makedirs(dataset_dir, exist_ok=True)
        part_count = 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=RAW_BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                local_file = os.path.join(dataset_dir, f"part-{part_count:05d}.parquet")
                s3.download_file(RAW_BUCKET_NAME, key, local_file)
                local_files.append((dataset, local_file))
                part_count += 1
        if part_count == 0:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise FileNotFoundError(f"No parquet file found for {dataset} batch {batch_date}")

    return local_files


def upload_to_internal_stage(connection, local_files, batch_date):
    with connection.cursor() as cursor:
        cleared_datasets = set()
        for dataset, local_file in local_files:
            stage_path = f"@{STAGE_NAME}/{dataset}/batch_date={batch_date}/"
            if dataset not in cleared_datasets:
                cursor.execute(f"REMOVE {stage_path};")
                cleared_datasets.add(dataset)
            cursor.execute(
                f"""
PUT file://{local_file}
{stage_path}
AUTO_COMPRESS=FALSE
OVERWRITE=TRUE;
"""
            )

            print(f"{dataset} uploaded successfully.")


def test_connection():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA(),
                    CURRENT_USER();
                """
            )
            result = cursor.fetchone()
            print(result)

    finally:
        connection.close()


def main(batch_date):
    connection = get_connection()
    temp_dir = None

    try:
        local_files = download_silver_files(batch_date)
        temp_dir = os.path.dirname(os.path.dirname(local_files[0][1]))
        upload_to_internal_stage(connection, local_files, batch_date)
        print("All datasets uploaded successfully.")

    finally:
        connection.close()
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    batch_date = "2026-07-12"
    main(batch_date)
