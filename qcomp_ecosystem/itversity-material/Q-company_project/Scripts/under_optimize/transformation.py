from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Dict, List, Callable
from datetime import datetime
import re

# Data transformation functions
class DataTransformer:
    @staticmethod
    def rename_columns(df: DataFrame) -> DataFrame:
        return df.withColumnRenamed("name", "sales_agent_name") \
                 .withColumnRenamed("hire_date", "sales_agent_hire_date") \
                 .withColumnRenamed("location", "branch_location") \
                 .withColumnRenamed("establish_date", "branch_establish_date") \
                 .withColumnRenamed("class", "branch_class") 
                
    @staticmethod
    def remove_blank_columns(df: DataFrame) -> DataFrame:
        return df.select([c for c in df.columns if df.filter(col(c).isNotNull()).count() > 0])

    @staticmethod
    def map_offers_to_discount(spark: SparkSession, df: DataFrame, offers_dict: Dict[str, float]) -> DataFrame:
        broadcast_offers = spark.sparkContext.broadcast(offers_dict)
        offer_columns = ["offer_1", "offer_2", "offer_3", "offer_4", "offer_5"]
        
        def create_offer_column(offer_col: str):
            return when(col(offer_col) == lit(True), lit(broadcast_offers.value[offer_col]))
        
        offer_discount_columns = [create_offer_column(offer_col).alias(f"{offer_col}_discount") for offer_col in offer_columns]
        df_with_offer_discounts = df.select("*", *offer_discount_columns)
        discount_column = coalesce(*[col(f"{offer_col}_discount") for offer_col in offer_columns], lit(broadcast_offers.value["null"]))
        
        return df_with_offer_discounts.withColumn("discount", discount_column.cast(FloatType())) \
                                      .drop(*[f"{offer_col}_discount" for offer_col in offer_columns]) \
                                      .drop(*offer_columns)

    @staticmethod
    def merge_customer_name(df: DataFrame) -> DataFrame:
        return df.withColumn("customer_name", concat(col("customer_fname"), lit(" "), col("customer_lname"))) \
                 .drop("customer_fname", "customer_lname")

    @staticmethod
    @udf(returnType=StringType())
    def clean_email(email: str) -> str:
        if email is None:
            return None
        email = email.strip()
        com = email.rfind('.')
        email = email[:com+1] + "com"
        email = re.sub(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}).*', r'\1', email)
        return email if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) else None

    @staticmethod
    @udf(returnType=StringType())
    def validate_transaction_id(trx_id: str) -> str:
        if trx_id is None:
            return None
        trx_id = trx_id.strip()
        numeric_part = re.sub(r'\D', '', trx_id)
        return f"trx-{numeric_part}" if numeric_part else None

    @staticmethod
    @udf(returnType=DoubleType())
    def validate_unit_price(price):
        return price if price >= 0 else (-1 * price)
    
    @staticmethod    
    def rearrange_columns(df: DataFrame) -> DataFrame:
        new_order = [
            'transaction_id', 'transaction_date', 'customer_id', 'customer_name', 'customer_email',
            'product_id', 'product_name', 'product_category', 'units', 'unit_price', 'discount',
            'payment_method', 'group', 'is_online', 'sales_agent_id', 'sales_agent_name',
            'sales_agent_hire_date', 'branch_id', 'branch_location', 'branch_class',
            'shipping_street_name', 'shipping_city', 'shipping_state', 'shipping_zip_code'
        ]
        return df.select(new_order)

    @staticmethod
    def convert_dates_to_date_type(df: DataFrame) -> DataFrame:
        return df.withColumn('transaction_date', to_date(col('transaction_date'))) \
                 .withColumn('branch_establish_date', to_date(col('branch_establish_date'))) \
                 .withColumn('sales_agent_hire_date', to_date(col('sales_agent_hire_date')))

    @staticmethod
    def convert_ids_to_long_type(df: DataFrame) -> DataFrame:
        return df.withColumn('sales_agent_id', col('sales_agent_id').cast(LongType())) \
                 .withColumn('branch_id', col('branch_id').cast(LongType()))

    @staticmethod
    def split_shipping_address(df: DataFrame) -> DataFrame:
        return df.withColumn("shipping_address_split", split(col("shipping_address"), "/")) \
                 .withColumn("shipping_street_name", col("shipping_address_split")[0]) \
                 .withColumn("shipping_city", col("shipping_address_split")[1]) \
                 .withColumn("shipping_state", col("shipping_address_split")[2]) \
                 .withColumn("shipping_zip_code", col("shipping_address_split")[3]) \
                 .drop("shipping_address", "shipping_address_split")
    
    @staticmethod
    def calculate_total_price(df: DataFrame) -> DataFrame:
        return df.withColumn("total_price", round(col("units") * col("unit_price") * (1 - col("discount")), 3))

    @staticmethod
    def map_shipping_state(spark: SparkSession, df: DataFrame, state_dict: Dict[str, str]) -> DataFrame:
        broadcast_dict = spark.sparkContext.broadcast(state_dict)
        conditions = coalesce(*[when(col("shipping_state") == key, lit(value)) for key, value in broadcast_dict.value.items()])
        return df.withColumn("shipping_state_mapped", when(conditions.isNotNull(), conditions).otherwise(col("shipping_state"))) \
                 .drop("shipping_state") \
                 .withColumnRenamed("shipping_state_mapped", "shipping_state")