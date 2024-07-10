from pyspark.sql import SparkSession, DataFrame
from clean import DataQualityLayer
from io_operations import DataReader, DataWriter
from utils import Schemas, Config

# Main execution
def main():
    spark = SparkSession.builder.appName("DataQualityLayer").getOrCreate()

    # Read data
    raw_df = DataReader.read_latest_parquet(spark, Config.HDFS_BASE_PATH)
    if raw_df is None:
        return

    # Initialize DataQualityLayer
    dq_layer = DataQualityLayer(spark)

    # Apply common transformations
    common_df = dq_layer.apply_common_transformations(raw_df)

    # Split into online and offline
    split_dfs = dq_layer.split_online_offline(common_df)

    # Apply specific transformations, add row index, and write data
    for df_type, df in split_dfs.items():
        if df_type == "offline":
            df = dq_layer.apply_offline_transformations(df)
            df = df.select(Schemas.offline_transactions.fieldNames())
        else:  # online
            df = dq_layer.apply_online_transformations(df)
            df = df.select(Schemas.online_transactions.fieldNames())
        
        df_with_index = dq_layer.add_row_index(df)
        
        # Write the data using the new DataWriter method
        DataWriter.write_parquet(spark, df_with_index, Config.CLEANED_BASE_PATH, df_type, ["transaction_date"])

    spark.stop()

if __name__ == "__main__":
    main()