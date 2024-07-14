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

def write_parquet(spark, df: DataFrame, path: str, on: str) -> None:
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    merged_file_exists = fs.exists(file_path)
    print(path)
    print(merged_file_exists)
    if merged_file_exists:
        existing_df = spark.read.parquet(path)
        changes_records = df.join(existing_df, on=on, how="left_anti")
        changes_records.write.mode('append').parquet(path)
        print(f"Appended {changes_records.count()} rows to {file_path}")
    else:
        merged_df = df
        merged_df.write.mode('overwrite').parquet(path)
        print(f"Appended {merged_df.count()} rows to {file_path}")
        print(merged_df.printSchema())

    

def add_surrogate_key(df: DataFrame, key_name: str, order_by: str) -> DataFrame:
    window_spec = Window.orderBy(order_by)
    return df.withColumn(key_name, row_number().over(window_spec))

def process_date_dimension(spark) -> DataFrame:
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
    
    online_fact = online_fact.join(date_df.select('date_key', 'date'), online_fact["transaction_date"] == date_df["date"], "inner") \
                             .join(product_df.select('product_key', 'product_id'), "product_id") \
                             .join(customer_df.select('customer_key', 'customer_id'), "customer_id")
        
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
    
    offline_fact = offline_fact.join(date_df.select('date_key', 'date'), offline_fact["transaction_date"] == date_df["date"], "inner") \
                               .join(product_df.select('product_key', 'product_id'), "product_id") \
                               .join(customer_df.select('customer_key', 'customer_id'), "customer_id") \
                               .join(branch_df.select('branch_key', 'branch_id'), "branch_id") \
                               .join(sales_agent_df.select('sales_agent_key', 'sales_agent_id'), "sales_agent_id")
    
    return offline_fact.select(
        "transaction_id", "customer_key", "sales_agent_key", "branch_key", "product_key", "date_key",
        "units", "unit_price", "discount", "payment_method", "total_price", date_format("transaction_date", "yyyy-MM-dd").alias("transaction_date")
    )

def process_dimensions(spark: SparkSession, all_df: DataFrame, offline_df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    date_df = process_date_dimension(spark)
    product_df = process_product_dimension(all_df)
    customer_df = process_customer_dimension(all_df)
    branch_df = process_branch_dimension(offline_df)
    sales_agent_df = process_sales_agent_dimension(offline_df)
    
    return date_df, product_df, customer_df, branch_df, sales_agent_df

def save_dimensions(spark, dimensions: List[Tuple[str, DataFrame]]) -> None:
    for name, df, key in dimensions:
        write_parquet(spark, df, f"{Config.CONFORMED_NORMALIZED_BASE_PATH}/{name}/{name}", key)

def main():
    spark = create_spark_session()
    
    try:
        online_df = read_parquet(spark, f"{Config.CONFORMED_DENORMALIZED_BASE_PATH}/online_fact_table/online_merged")
        offline_df = read_parquet(spark, f"{Config.CONFORMED_DENORMALIZED_BASE_PATH}/offline_fact_table/offline_merged")
        all_df = read_parquet(spark, f"{Config.CONFORMED_DENORMALIZED_BASE_PATH}/all_sales_fact_table/sales_merged")
        print("get denorm data")
        
        date_df, product_df, customer_df, branch_df, sales_agent_df = process_dimensions(spark, all_df, offline_df)
        print("process dim")
        
        dimensions = [
            ("product_dim", product_df, "product_id"),
            ("customer_dim", customer_df, "customer_id"),
            ("branch_dim", branch_df, "branch_id"),
            ("sales_agent_dim", sales_agent_df, "sales_agent_id")
        ]
        save_dimensions(spark, dimensions)
        print("save dim")
        
        online_fact = process_online_fact(online_df, date_df, product_df, customer_df)
        offline_fact = process_offline_fact(offline_df, date_df, product_df, customer_df, branch_df, sales_agent_df)
        print("build facts")
        
        write_parquet(spark, online_fact, f"{Config.CONFORMED_NORMALIZED_BASE_PATH}/online_sales_fact/online_fact", "transaction_id")
        write_parquet(spark, offline_fact, f"{Config.CONFORMED_NORMALIZED_BASE_PATH}/offline_sales_fact/offline_fact", "transaction_id")
        
        print("Normalized model processing completed successfully.")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
