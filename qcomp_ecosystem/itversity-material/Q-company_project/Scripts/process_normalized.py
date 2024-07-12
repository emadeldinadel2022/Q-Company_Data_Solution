from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, row_number
from pyspark.sql.window import Window
from functools import reduce
from typing import List, Tuple
from utils import Config
import os


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("Normalized_Model") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode('append').parquet(path)

def add_surrogate_key(df: DataFrame, key_name: str, order_by: str) -> DataFrame:
    window_spec = Window.orderBy(order_by)
    return df.withColumn(key_name, row_number().over(window_spec))

def process_date_dimension(spark: SparkSession) -> DataFrame:
       return read_parquet(spark, "/user/itversity/q-company_conformed_layer/normalized_model/date_dim/date_dim_table")

def process_product_dimension(df: DataFrame) -> DataFrame:
    product_dim = df.select("product_id", "product_name", "product_category", "unit_price").distinct()
    return add_surrogate_key(product_dim, "product_key", "product_id")

def process_customer_dimension(df: DataFrame) -> DataFrame:
    customer_dim = df.select("customer_id", "customer_name", "customer_email").distinct()
    return add_surrogate_key(customer_dim, "customer_key", "customer_id")

def process_branch_dimension(df: DataFrame) -> DataFrame:
    branch_dim = df.select("branch_id", "branch_location", "branch_establish_date", "branch_class").distinct()
    return add_surrogate_key(branch_dim, "branch_key", "branch_id")

def process_sales_agent_dimension(df: DataFrame) -> DataFrame:
    sales_agent_dim = df.select("sales_agent_id", "sales_agent_hire_date", "sales_agent_name").distinct()
    return add_surrogate_key(sales_agent_dim, "sales_agent_key", "sales_agent_id")

def process_online_fact(online_df: DataFrame, date_df: DataFrame, product_df: DataFrame, customer_df: DataFrame) -> DataFrame:
    online_fact = online_df.select(
        "transaction_id", "customer_id", "product_id", "units", "unit_price", "discount",
        "payment_method", "group", "total_price", "shipping_zip_code", "shipping_state",
        "shipping_city", "shipping_street_name", date_format("transaction_date", "yyyy-MM-dd").alias("transaction_date")
    )
    
    # Join with dimension tables
    online_fact = online_fact.join(date_df.select('date_key', 'date'), online_fact["transaction_date"] == date_df["date"], "inner") \
                             .join(product_df.select('product_key', 'product_id'), "product_id") \
                             .join(customer_df.select('customer_key', 'customer_id'), "customer_id")
        
    # Select final columns
    return online_fact.select(
        "transaction_id", "customer_key", "product_key", "date_key", "units", "unit_price", "discount",
        "payment_method", "group", "total_price", "shipping_zip_code", "shipping_state",
        "shipping_city", "shipping_street_name"
    )

def process_offline_fact(offline_df: DataFrame, date_df: DataFrame, product_df: DataFrame, customer_df: DataFrame, branch_df: DataFrame, sales_agent_df: DataFrame) -> DataFrame:
    offline_fact = offline_df.select(
        "transaction_id", "customer_id", "sales_agent_id", "branch_id", "product_id",
        "units", "unit_price", "discount", "payment_method", "total_price",
        date_format("transaction_date", "yyyy-MM-dd").alias("transaction_date")
    )
    
    # Join with dimension tables
    offline_fact = offline_fact.join(date_df.select('date_key', 'date'), offline_fact["transaction_date"] == date_df["date"], "inner") \
                               .join(product_df.select('product_key', 'product_id'), "product_id") \
                               .join(customer_df.select('customer_key', 'customer_id'), "customer_id") \
                               .join(branch_df.select('branch_key', 'branch_id'), "branch_id") \
                               .join(sales_agent_df.select('sales_agent_key', 'sales_agent_id'), "sales_agent_id")
    
    # Select final columns
    return offline_fact.select(
        "transaction_id", "customer_key", "sales_agent_key", "branch_key", "product_key", "date_key",
        "units", "unit_price", "discount", "payment_method", "total_price", date_format("transaction_date", "yyyy-MM-dd").alias("transaction_date")
    )

def process_dimensions(all_df: DataFrame, offline_df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    date_df = process_date_dimension(spark)
    product_df = process_product_dimension(all_df)
    customer_df = process_customer_dimension(all_df)
    branch_df = process_branch_dimension(offline_df)
    sales_agent_df = process_sales_agent_dimension(offline_df)
    
    return date_df, product_df, customer_df, branch_df, sales_agent_df

def save_dimensions(dimensions: List[Tuple[str, DataFrame]]) -> None:
    for name, df in dimensions:
        write_parquet(df, f"{Config.CONFORMED_NORMALIZED_BASE_PATH}/{name}/{name}")

def main():
    spark = create_spark_session()
    
    try:
        # Read denormalized data
        online_df = read_parquet(spark, f"{Config.CONFORMED_DENORMALIZED_BASE_PATH}/online_fact_table/online_merged")
        offline_df = read_parquet(spark, f"{Config.CONFORMED_DENORMALIZED_BASE_PATH}/offline_fact_table/offline_merged")
        all_df = read_parquet(spark, f"{Config.CONFORMED_DENORMALIZED_BASE_PATH}/all_sales_fact_table/sales_merged")
        
        # Process dimensions
        date_df, product_df, customer_df, branch_df, sales_agent_df = process_dimensions(all_df, offline_df)
        
        # Save dimensions
        dimensions = [
            ("product_dim", product_df),
            ("customer_dim", customer_df),
            ("branch_dim", branch_df),
            ("sales_agent_dim", sales_agent_df)
        ]
        save_dimensions(dimensions)
        
        # Process and save fact tables
        online_fact = process_online_fact(online_df, date_df, product_df, customer_df)
        offline_fact = process_offline_fact(offline_df, date_df, product_df, customer_df, branch_df, sales_agent_df)
        
        write_parquet(online_fact, f"{Config.CONFORMED_NORMALIZED_BASE_PATH}/online_sales_fact/online_fact")
        write_parquet(offline_fact, f"{Config.CONFORMED_NORMALIZED_BASE_PATH}/offline_sales_fact/offline_fact")
        
        print("Normalized model processing completed successfully.")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


