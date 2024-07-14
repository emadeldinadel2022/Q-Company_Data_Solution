from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List
from pyspark.sql.column import Column
import re
from utils import *
from io_operations import *

class TransformationLayer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.state_dict = spark.sparkContext.broadcast({
            'AZ': 'Arizona', 'DC': 'District of Columbia', 'KY': 'Kentucky',
            'CA': 'California', 'CT': 'Connecticut', 'VT': 'Vermont',
            'MD': 'Maryland', 'AL': 'Alabama', 'TN': 'Tennessee',
            'GA': 'Georgia', 'MA': 'Massachusetts', 'FL': 'Florida',
            'CO': 'Colorado', 'AK': 'Alaska', 'AR': 'Arkansas',
            'OK': 'Oklahoma', 'Washington': 'Washington'
        })
        self.offers_dict = spark.sparkContext.broadcast({
            "null": 0.0, "offer_1": 0.05, "offer_2": 0.1,
            "offer_3": 0.15, "offer_4": 0.20, "offer_5": 0.25
        })
        
    def calculate_discount(self, df: DataFrame) -> Column:
        offers = self.offers_dict.value
        return coalesce(
            *[when(col(f"offer_{i}") == lit(True), lit(offers[f"offer_{i}"])) for i in range(1, 6)],
            lit(offers["null"])
        ).cast(FloatType()).alias("discount")

    def split_shipping_address(self, df: DataFrame) -> List[Column]:
        return [
            split(col("shipping_address"), "/").getItem(0).alias("shipping_street_name"),
            split(col("shipping_address"), "/").getItem(1).alias("shipping_city"),
            self.map_shipping_state(split(col("shipping_address"), "/").getItem(2)).alias("shipping_state"),
            split(col("shipping_address"), "/").getItem(3).alias("shipping_zip_code")
        ]

    def map_shipping_state(self, state_col: Column) -> Column:
        return coalesce(*[
            when(state_col == lit(key), lit(value))
            for key, value in self.state_dict.value.items()
        ], state_col)
    
    def clean_email(self, email_col):
        return when(email_col.isNotNull(),
                    regexp_replace(
                            regexp_replace(email_col, r'\s+', ''),  # Remove whitespace
                                r'(.+)@(.+)\.[^.]+', '$1@$2.com'),  # Replace last part with .com
                    ).otherwise(None)


    def validate_transaction_id(self, trx_id_col):
        return when(trx_id_col.isNotNull(),
                    when(regexp_replace(trx_id_col, r'\D', '') != '',
                         concat(lit('trx-'), regexp_replace(trx_id_col, r'\D', '')))
                    .otherwise(None)
               ).otherwise(None)


    def transformations(self, df: DataFrame) -> DataFrame:
        discount = self.calculate_discount(df)

        shipping_address_columns = self.split_shipping_address(df)

        transformed_df = df.select(
            self.validate_transaction_id(col("transaction_id")).alias("transaction_id"),
            to_date(col("transaction_date")).alias("transaction_date"),
            col("customer_id"),
            concat(col("customer_fname"), lit(" "), col("customer_lname")).alias("customer_name"),
            self.clean_email(col("customer_email")).alias("customer_email"),
            col("product_id"),
            col("product_name"),
            col("product_category"),
            col("units"),
            col("unit_price"),
            discount.alias("discount"),
            col("payment_method"),
            col("group"),
            when(col("is_online") == "yes", "online").otherwise("offline").alias("transaction_type"),
            col("sales_agent_id").cast(LongType()),
            col("name").alias("sales_agent_name"),
            to_date(col("hire_date")).alias("sales_agent_hire_date"),
            col("branch_id").cast(LongType()),
            col("location").alias("branch_location"),
            col("class").alias("branch_class"),
            col("establish_date").alias("branch_establish_date"),
            *shipping_address_columns
        )

        return transformed_df.withColumn("total_price", round(col("units") * col("unit_price") * (1 - col("discount")), 3))
            

def main():    
    spark = SparkSession.builder.appName("OptimizedDataQualityLayer").getOrCreate()
    
    raw_df = DataReader.read_latest_csv(spark, Config.RAW_BASE_PATH, Schemas.sales_transcation_schema, 200)
    
    if raw_df is None:
        return

    raw_df = raw_df.repartition(200, "transaction_date").cache()
    raw_df.count()  

    dq_layer = TransformationLayer(spark)
    
    transformed_df = dq_layer.transformations(raw_df)
    
    online_df = transformed_df.filter(col("transaction_type") == "online").select(Schemas.online_sales_transcation_schema.fieldNames())
    offline_df = transformed_df.filter(col("transaction_type") == "offline").select(Schemas.offline_sales_transcation_schema.fieldNames())

    DataWriter.write_parquet(spark, online_df, Config.STANDARDIZED_BASE_PATH, "online", ["transaction_date"])
    DataWriter.write_parquet(spark, offline_df, Config.STANDARDIZED_BASE_PATH, "offline", ["transaction_date"])


if __name__ == "__main__":
    main()