import os
import tempfile
import polars as pl


def main():
    df_raw_items = pl.read_csv('./data/raw/raw_items').rename({"sku": "product_id", "id": "item_id"})
    df_raw_products = pl.read_csv('./data/raw/raw_products').rename({"sku": "product_id"}).with_columns(
        [pl.when(pl.col("type") == 'food').then(1).otherwise(0).alias("is_food_item"),
         pl.when(pl.col("type") == 'beverage').then(1).otherwise(0).alias("is_drink_item")
         ])


    df = df_raw_products.join(df_raw_items, on="product_id", how="left").groupby('order_id') \
        .agg(
        pl.col("is_food_item").sum().alias("count_food"),
        pl.col("is_drink_item").sum().alias("count_drink"),
        pl.count().alias("total_count")
    )

    df.write_parquet('./data/out/polars.parquet')


if __name__ == '__main__':
    main()
