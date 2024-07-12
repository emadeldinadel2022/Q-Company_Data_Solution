from pyspark.sql import SparkSession, DataFrame
from cleansing import DataQualityLayer
from io_operations import DataReader, DataWriter
from utils import Config, Schemas, HDFSUtils

# Main execution
def main():
    spark = SparkSession.builder.appName("DataQualityLayer").getOrCreate()
    
    expected_schema = Schemas.sales_transcation_schema
    # Read data
    raw_df = DataReader.read_latest_csv(spark, Config.RAW_BASE_PATH, expected_schema, 10)    
    
    if raw_df is None:
        return

    # Initialize DataQualityLayer
    dq_layer = DataQualityLayer(spark)
    
    # Apply common transformations
    transformed_df = dq_layer.apply_common_transformations(raw_df)
    
    # Split into online and offline
    split_dfs = dq_layer.split_online_offline(transformed_df)
    
    # Apply specific transformations, add row index, and write data
    for df_type, df in split_dfs.items():
        if df_type == "offline":
            df = dq_layer.apply_offline_transformations(df)
            df = df.select(Schemas.offline_sales_transcation_schema.fieldNames())
        else:  # online
            df = dq_layer.apply_online_transformations(df)
            df = df.select(Schemas.online_sales_transcation_schema.fieldNames())
                
        # Write the data using the new DataWriter method
        DataWriter.write_parquet(spark, df, Config.STANDARDIZED_BASE_PATH, df_type, ["transaction_date"])

    spark.stop()

if __name__ == "__main__":
    main()