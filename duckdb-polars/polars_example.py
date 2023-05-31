import os
import boto3
import tempfile
import s3fs
import polars as pl

key = os.environ['AWS_ACCESS_KEY_ID']
secret = os.environ['AWS_SECRET_ACCESS_KEY']
session_token = os.environ['AWS_SESSION_TOKEN']


def main():
    s3 = boto3.client('s3', region_name='eu-west-1')

    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as raw_items:
        # Download the CSV file from S3
        s3.download_fileobj("conveyor-samples-b9a6edf0", "coffee-data/raw/raw_items.csv", raw_items)
        raw_items.flush()
        df_raw_items = pl.read_csv(raw_items.name).rename({"sku": "product_id", "id": "item_id"})

    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as raw_products:
        # Download the CSV file from S3
        s3.download_fileobj("conveyor-samples-b9a6edf0", "coffee-data/raw/raw_products.csv", raw_products)
        raw_products.flush()

        df_raw_products = pl.read_csv(raw_products.name).rename({"sku": "product_id"}).with_columns(
            [pl.when(pl.col("type") == 'food').then(1).otherwise(0).alias("is_food_item"),
             pl.when(pl.col("type") == 'beverage').then(1).otherwise(0).alias("is_drink_item")
             ])

    df = df_raw_products.join(df_raw_items, on="product_id", how="left").groupby('order_id').agg(
        pl.col("is_food_item").sum().alias("count_food"),
        pl.col("is_drink_item").sum().alias("count_drink"),
        pl.count().alias("total_count")
    )

    fs = s3fs.S3FileSystem()
    with fs.open('conveyor-samples-b9a6edf0/coffee-data/tmp/polars_output.parquet', mode='wb') as f:
        df.write_parquet(f)


if __name__ == '__main__':
    main()
