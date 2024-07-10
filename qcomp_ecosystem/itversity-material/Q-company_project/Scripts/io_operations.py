from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Callable
from datetime import datetime
from utils import HDFSUtils
import os

# Data reading
class DataReader:
    @staticmethod
    def read_latest_parquet(spark: SparkSession, base_path: str) -> DataFrame:
        current_date = datetime.now().strftime("%Y-%m-%d")
        hdfs_path = f"{base_path}/sales_transactions_{current_date}"
        latest_file = HDFSUtils.get_latest_file(spark, hdfs_path)
        if latest_file:
            print(f"Processing file: {latest_file}")
            return spark.read.option("mergeSchema", "true").parquet(latest_file)
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

        group_number = df.select("group").distinct().collect()
            
        file_name = f"{transaction_type}_transactions_{group_number}_{timestamp}"
        group_path = os.path.join(full_path, file_name)

        df.write \
            .partitionBy(partition_cols) \
            .mode("overwrite") \
            .parquet(group_path)

        print(f"Written {transaction_type} transactions for group {group_number} to {group_path}")