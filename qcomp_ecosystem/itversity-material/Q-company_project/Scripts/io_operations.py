from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Callable
from datetime import datetime
from utils import HDFSUtils
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit
import os

# Data reading
class DataReader:
    @staticmethod
    def read_latest_csv(spark: SparkSession, base_path: str, expected_schema: StructType, num_partitions: int = 200) -> DataFrame:
        current_date = datetime.now().strftime("%Y-%m-%d")
        hdfs_path = f"{base_path}/raw_sales_transactions_{current_date}"
        latest_file = HDFSUtils.get_latest_file(spark, hdfs_path)
        max_retries = 5
        initial_wait_time = 5
        
        if latest_file:
            print(f"Processing file: {latest_file}")
            for attempt in range(max_retries):
                try:
                    df = spark.read.option("header", "true").csv(latest_file)
                    
                    actual_fields = set(df.columns)
                    expected_fields = set(field.name for field in expected_schema.fields)

                    missing_fields = expected_fields - actual_fields
                    new_fields = actual_fields - expected_fields

                    for field in missing_fields:
                        df = df.withColumn(field, lit(None))

                    select_expr = []
                    for field in expected_schema.fields:
                        if field.name in actual_fields:
                            select_expr.append(col(field.name).cast(field.dataType).alias(field.name))
                        else:
                            select_expr.append(lit(None).cast(field.dataType).alias(field.name))

                    for field in new_fields:
                        select_expr.append(col(field).alias(field))

                    df_with_schema = df.select(select_expr)

                    repartitioned_df = df_with_schema.repartition(num_partitions)
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

        group_number = str(df.select("group").filter("group is not null").first()[0])
        
        
        file_name = f"{transaction_type}_transactions_{group_number}_{timestamp}"
        group_path = os.path.join(full_path, file_name)
        
        schema = df.schema

        df.write \
            .option("schema", schema.json()) \
            .partitionBy(partition_cols) \
            .mode("overwrite") \
            .parquet(group_path)
        
        print(f"Written {transaction_type} transactions for group {group_number} to {group_path}")
        
        schema_path = os.path.join(group_path, "_schema")
        spark.createDataFrame([], schema).limit(0).write.mode("overwrite").parquet(schema_path)