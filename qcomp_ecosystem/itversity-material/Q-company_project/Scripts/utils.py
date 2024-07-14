from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from datetime import datetime

class Config:
    RAW_BASE_PATH = "/user/itversity/q-company_raw_layer"
    STANDARDIZED_BASE_PATH = "/user/itversity/q-company_standardized_layer"
    CONFORMED_NORMALIZED_BASE_PATH = "/user/itversity/q-company_conformed_layer/normalized_model"
    CONFORMED_DENORMALIZED_BASE_PATH = "/user/itversity/q-company_conformed_layer/denormalized_model"
    DATE_DIM_PATH = "/user/itversity/q-company_conformed_layer/normalized_model/date_dim/date_dim_table"
    
class Schemas:
    sales_transcation_schema = StructType([
            StructField("transaction_date", DateType(), nullable=False),
            StructField("transaction_id", StringType(), nullable=False),
            StructField("customer_id", LongType(), nullable=False),
            StructField("customer_fname", StringType(), nullable=False),
            StructField("customer_lname", StringType(), nullable=False),
            StructField("customer_email", StringType(), nullable=False),
            StructField("sales_agent_id", LongType(), nullable=True),
            StructField("branch_id", LongType(), nullable=True),
            StructField("product_id", LongType(), nullable=False),
            StructField("product_name", StringType(), nullable=False),
            StructField("product_category", StringType(), nullable=False),
            StructField("offer_1", StringType(), nullable=True),
            StructField("offer_2", StringType(), nullable=True),
            StructField("offer_3", StringType(), nullable=True),
            StructField("offer_4", StringType(), nullable=True),
            StructField("offer_5", StringType(), nullable=True),
            StructField("units", LongType(), nullable=False),
            StructField("unit_price", DoubleType(), nullable=False),
            StructField("is_online", StringType(), nullable=False),
            StructField("payment_method", StringType(), nullable=False),
            StructField("shipping_address", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("hire_date", DateType(), nullable=True),
            StructField("location", StringType(), nullable=True),
            StructField("establish_date", DateType(), nullable=True),
            StructField("class", StringType(), nullable=True),
            StructField("group", StringType(), nullable=False)
        ])

    offline_sales_transcation_schema = StructType([
            StructField("transaction_date", DateType(), nullable=False),
            StructField("transaction_id", StringType(), nullable=False),
            StructField("customer_id", LongType(), nullable=False),
            StructField("customer_name", StringType(), nullable=False),
            StructField("customer_email", StringType(), nullable=False),
            StructField("sales_agent_id", LongType(), nullable=False),
            StructField("branch_id", LongType(), nullable=False),
            StructField("product_id", LongType(), nullable=False),
            StructField("product_name", StringType(), nullable=False),
            StructField("product_category", StringType(), nullable=False),
            StructField("units", IntegerType(), nullable=False),
            StructField("unit_price", DoubleType(), nullable=False),
            StructField("discount", FloatType(), nullable=False),
            StructField("total_price", DoubleType(), nullable=False),
            StructField("payment_method", StringType(), nullable=False),
            StructField("sales_agent_name", StringType(), nullable=False),
            StructField("sales_agent_hire_date", DateType(), nullable=False),
            StructField("branch_location", StringType(), nullable=False),
            StructField("branch_establish_date", DateType(), nullable=False),
            StructField("branch_class", StringType(), nullable=False),
            StructField("group", StringType(), nullable=False)
        ])

    online_sales_transcation_schema = StructType([
        StructField("transaction_date", DateType(), nullable=False),
        StructField("transaction_id", StringType(), nullable=False),
        StructField("customer_id", LongType(), nullable=False),
        StructField("customer_name", StringType(), nullable=False),
        StructField("customer_email", StringType(), nullable=False),
        StructField("product_id", LongType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("product_category", StringType(), nullable=False),
        StructField("units", IntegerType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("discount", FloatType(), nullable=False),
        StructField("total_price", DoubleType(), nullable=False),
        StructField("payment_method", StringType(), nullable=False),
        StructField("shipping_street_name",  StringType(), nullable=False), 
        StructField("shipping_city",  StringType(), nullable=False),
        StructField("shipping_state",  StringType(), nullable=False),
        StructField("shipping_zip_code",  StringType(), nullable=False),
        StructField("group", StringType(), nullable=False)
    ])
    
class HDFSUtils:
    @staticmethod
    def get_latest_file(spark: SparkSession, hdfs_path: str) -> str:
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))
        sorted_files = sorted(files, key=lambda f: f.getModificationTime(), reverse=True)
        return sorted_files[0].getPath().toString() if sorted_files else None
    