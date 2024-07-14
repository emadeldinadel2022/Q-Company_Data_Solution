import os
from pyspark.sql.functions import lit
from typing import Tuple
from datetime import datetime
from utils import *

def get_latest_parquet_file(spark, hdfs_path, file_prefix):
    try:
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
                    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))
        
        parquet_files = [f.getPath().toString() for f in files if f.getPath().getName().startswith(file_prefix)]
        
        if not parquet_files:
            print(f"No matching Parquet files found in {hdfs_path}")
            return None
        
        #sorted_files = sorted(files, key=lambda f: f.getModificationTime(), reverse=True)
        #latest_file = sorted_files[0].getPath().toString() if sorted_files else None
        latest_file = max(parquet_files, key=lambda x: os.path.basename(x))
        
        return latest_file
    
    except Exception as e:
        print(f"Error accessing path {hdfs_path}: {str(e)}")
        print("Directory contents:")
        try:
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))
            for fileStatus in status:
                print(fileStatus.getPath().toString())
        except Exception as inner_e:
            print(f"Unable to list directory contents: {str(inner_e)}")
        return None

def align_schemas(df1: DataFrame, df2: DataFrame) -> Tuple[DataFrame, DataFrame]:
    columns1 = set(df1.columns)
    columns2 = set(df2.columns)
    
    df2 = df2.select(*df2.columns, *[lit(None).alias(col) for col in columns1 - columns2])
    df1 = df1.select(*df1.columns, *[lit(None).alias(col) for col in columns2 - columns1])
    
    all_columns = sorted(list(columns1.union(columns2)))
    return df1.select(all_columns), df2.select(all_columns)

def read_parquet_with_schema(spark: SparkSession, path: str) -> DataFrame:
    schema_path = os.path.join(path, "_schema")
    schema_df = spark.read.parquet(schema_path)
    schema = schema_df.schema
    return spark.read.schema(schema).parquet(path)

def process_denormalized_model(spark: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    current_day = datetime.now().strftime("%Y-%m-%d")
    input_base_path = f"{Config.STANDARDIZED_BASE_PATH}/standardized_sales_transaction_{current_day}"
    
    # Get latest online file
    online_path = get_latest_parquet_file(spark, input_base_path, "online_transactions")
    if not online_path:
        raise ValueError(f"No online transaction files found in {input_base_path}")
    print(f"Latest online file: {online_path}")
    online_df = read_parquet_with_schema(spark, online_path)
    online_df_a = online_df.withColumn("transaction_type", lit("online"))
    
    offline_path = get_latest_parquet_file(spark, input_base_path, "offline_transactions")
    if not offline_path:
        raise ValueError(f"No offline transaction files found in {input_base_path}")
    print(f"Latest offline file: {offline_path}")
    offline_df = read_parquet_with_schema(spark, offline_path)
    offline_df_a = offline_df.withColumn("transaction_type", lit("offline"))

    all_df = get_all_sales(online_df_a, offline_df_a)
    
    return online_df_a, offline_df_a, all_df

def get_all_sales(online_df: DataFrame, offline_df: DataFrame) -> DataFrame:
    
    online_df_a, offline_df_a = align_schemas(online_df, offline_df)
    
    all_df = online_df_a.union(offline_df_a)
    
    new_order = [
        'transaction_id', 'transaction_date', 'transaction_type', 'customer_id', 'customer_name', 'customer_email',
        'product_id', 'product_name', 'product_category', 'units', 'unit_price', 'discount',
        'payment_method', 'group', 'sales_agent_id', 'sales_agent_name',
        'sales_agent_hire_date', 'branch_id', 'branch_location', 'branch_class',
        'shipping_street_name', 'shipping_city', 'shipping_state', 'shipping_zip_code'
    ]
    
    return all_df.select(new_order)

def denorm_modeling(spark: SparkSession, df: DataFrame, transaction_type: str) -> None:
    file_path = get_file_path(transaction_type)
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
    merged_file_exists = fs.exists(path)
    print(path)
    print(merged_file_exists)
    if merged_file_exists:
        existing_df = spark.read.parquet(file_path)
        changes_records = df.join(existing_df, on="transaction_id", how="left_anti")
        merged_df = existing_df.unionByName(changes_records)
        changes_records.write.option("schema", df.schema.json()).partitionBy(["transaction_date"]).mode("append").parquet(file_path)
    else:
        merged_df = df
        merged_df.write.option("schema", df.schema.json()).partitionBy(["transaction_date"]).mode("append").parquet(file_path)
        print(f"Appended {merged_df.count()} rows to {file_path}")
  
    print(merged_df.printSchema())

def get_file_path(transaction_type: str) -> str:
    denorm_path = Config.CONFORMED_DENORMALIZED_BASE_PATH
    if transaction_type == 'online':
        return f"{denorm_path}/online_fact_table/online_merged"
    elif transaction_type == 'offline':
        return f"{denorm_path}/offline_fact_table/offline_merged"
    else:
        return f"{denorm_path}/all_sales_fact_table/sales_merged"

def main():
    spark = SparkSession.builder.appName("DenormalizedModelProcessing").getOrCreate()
    
    try:
        online_df, offline_df, all_df = process_denormalized_model(spark)
        
        denorm_modeling(spark, online_df, 'online')
        denorm_modeling(spark, offline_df, 'offline')
        denorm_modeling(spark, all_df, 'all')
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
     main()