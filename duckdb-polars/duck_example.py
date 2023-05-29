import os
import duckdb

key = os.environ['AWS_ACCESS_KEY_ID']
secret = os.environ['AWS_SECRET_ACCESS_KEY']
session_token = os.environ['AWS_SESSION_TOKEN']


def main():
    duckdb.sql(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='eu-west-1';
        SET s3_access_key_id='{key}';
        SET s3_secret_access_key='{secret}';
        SET s3_session_token='{session_token}';
        
        CREATE VIEW items AS
        WITH 
        raw_items as (SELECT id as item_id, order_id, sku as product_id 
                           FROM read_csv('s3://conveyor-samples-b9a6edf0/coffee-data/raw/raw_items.csv', delim=',', header=True, AUTO_DETECT=True)),
        raw_products as (SELECT sku as product_id, name, price, description,
                 case when type = 'food' then 1 else 0 end is_food_item,
                 case when type = 'beverage' then 1 else 0 end is_drink_item
            FROM read_csv('s3://conveyor-samples-b9a6edf0/coffee-data/raw/raw_products.csv', delim=',', header=True, AUTO_DETECT=True))
        
        SELECT raw_items.order_id, 
               sum(raw_products.is_food_item) as count_food, 
               sum(raw_products.is_drink_item) as count_drink,
               count(*) as total_count
        FROM raw_products left join raw_items on raw_items.product_id = raw_products.product_id
        group by 1;
        
        COPY items TO 's3://conveyor-samples-b9a6edf0/coffee-data/tmp/duckdb_output.parquet' (FORMAT PARQUET);
	""")


if __name__ == '__main__':
    main()
