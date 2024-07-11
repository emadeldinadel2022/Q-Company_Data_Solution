from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Callable
from datetime import datetime
from utils import HDFSUtils
import os

# Data reading
class DataReader:
    @staticmethod
    def read_latest_csv(spark: SparkSession, base_path: str, num_partitions: int = 200) -> DataFrame:
        current_date = datetime.now().strftime("%Y-%m-%d")
        hdfs_path = f"{base_path}/raw_sales_transactions_{current_date}"
        latest_file = HDFSUtils.get_latest_file(spark, hdfs_path)
        max_retries = 5
        initial_wait_time = 5
        
        if latest_file:
            print(f"Processing file: {latest_file}")
            for attempt in range(max_retries):
                try:
                    # Read the CSV file with header and repartition
                    df = spark.read.option("header", "true").csv(latest_file)
                    repartitioned_df = df.repartition(num_partitions)
                    print(f"Successfully read CSV and partitioned to {num_partitions}")
                    return repartitioned_df
                except Exception as e:
                    wait_time = initial_wait_time * (2 ** attempt)
                    print(f"Attempt {attempt + 1} failed with error: {str(e)}")
                    print("Full stack trace:")
                    traceback.print_exc()
                    if attempt + 1 < max_retries:
                        print(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        print("Max retries reached. Could not read the CSV file.")
                        return None
        else:
            print(f"No files found in {hdfs_path}")
            return None
        
# Data Writer
class DataWriter:
    @staticmethod
    def write_parquet(spark: SparkSession, df: DataFrame, base_path: str, transaction_type: str, partition_cols: List[str]) -> None:
        current_date = datetime.now().strftime("%Y-%m-%d")
        standardized_dir = f"standardized_sales_transaction_{current_date}"
        full_path = os.path.join(base_path, standardized_dir)

        # Check if the standardized directory for the current day exists, create it if not
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(full_path)):
            fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(full_path))
            print(f"Created directory: {full_path}")
        else:
            print(f"Directory already exists: {full_path}")

        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        result_string = ''
        try:
            first_group = split_dfs['online'].select("group").filter("group is not null").first()[0]
            result_string = str(first_group)
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            result_string = None            
        
        file_name = f"{transaction_type}_transactions_{result_string}_{timestamp}"
        group_path = os.path.join(full_path, file_name)

        df.write \
            .partitionBy(partition_cols) \
            .mode("overwrite") \
            .parquet(group_path)

        print(f"Written {transaction_type} transactions for group {result_string} to {group_path}")