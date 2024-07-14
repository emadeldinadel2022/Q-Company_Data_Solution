from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Dict, List, Callable
from datetime import datetime
from transformation import DataTransformer
import re

# Data Quality Layer
class DataQualityLayer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.state_dict = {
            'AZ': 'Arizona', 'DC': 'District of Columbia', 'KY': 'Kentucky',
            'CA': 'California', 'CT': 'Connecticut', 'VT': 'Vermont',
            'MD': 'Maryland', 'AL': 'Alabama', 'TN': 'Tennessee',
            'GA': 'Georgia', 'MA': 'Massachusetts', 'FL': 'Florida',
            'CO': 'Colorado', 'AK': 'Alaska', 'AR': 'Arkansas',
            'OK': 'Oklahoma', 'Washington': 'Washington'
        }
        self.offers_dict = {
            "null": 0.0, "offer_1": 0.05, "offer_2": 0.1,
            "offer_3": 0.15, "offer_4": 0.20, "offer_5": 0.25
        }

    def split_online_offline(self, df: DataFrame) -> Dict[str, DataFrame]:
        return {
            "online": df.filter(col("is_online") == "yes").drop("is_online"),
            "offline": df.filter(col("is_online") == "no").drop("is_online")
        }

    def apply_common_transformations(self, df: DataFrame) -> DataFrame:
        transformations = [
            DataTransformer.rename_columns,
            DataTransformer.remove_blank_columns,
            lambda df: DataTransformer.map_offers_to_discount(self.spark, df, self.offers_dict),
            DataTransformer.merge_customer_name,
            lambda df: df.withColumn("customer_email", DataTransformer.clean_email(col("customer_email"))),
            lambda df: df.withColumn("transaction_id", DataTransformer.validate_transaction_id(col("transaction_id"))),
            DataTransformer.calculate_total_price
        ]

        for transform in transformations:
            df = transform(df)
        return df

    def apply_offline_transformations(self, df: DataFrame) -> DataFrame:
        transformations = [
            DataTransformer.convert_dates_to_date_type,
            DataTransformer.convert_ids_to_long_type,
        ]

        for transform in transformations:
            df = transform(df)
        return df

    def apply_online_transformations(self, df: DataFrame) -> DataFrame:
        transformations = [
            lambda df: df.withColumn('transaction_date', to_date(col('transaction_date'))),
            DataTransformer.split_shipping_address,
            lambda df: DataTransformer.map_shipping_state(self.spark, df, self.state_dict),
        ]

        for transform in transformations:
            df = transform(df)
        return df

    def add_row_index(self, df: DataFrame) -> DataFrame:
        return df.withColumn("row_index", monotonically_increasing_id())