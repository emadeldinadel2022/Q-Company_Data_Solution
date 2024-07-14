from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, dayofmonth, dayofweek, dayofyear, month, year, weekofyear, quarter, regexp_replace, date_format
from datetime import datetime, timedelta
from utils import Config

def create_date_dimension(spark: SparkSession) -> None:
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    date_df = spark.createDataFrame([(date,) for date in date_list], ["date"])
    
    date_df = date_df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
    
    date_df = date_df.withColumn("date_key", 
                                 regexp_replace(date_format(col("date"), "yyyyMMdd"), "-", "").cast("long"))
    
    date_df = date_df.withColumn("day", dayofmonth(col("date"))) \
        .withColumn("day_of_week", dayofweek(col("date"))) \
        .withColumn("day_of_year", dayofyear(col("date"))) \
        .withColumn("week_of_year", weekofyear(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("quarter", quarter(col("date"))) \
        .withColumn("year", year(col("date"))) \
        .withColumn("is_weekend", (col("day_of_week") >= 6).cast("integer")) \
        .withColumn("month_name", date_format(col("date"), "MMMM")) \
        .withColumn("day_name", date_format(col("date"), "EEEE")) \
        .withColumn("year_month", date_format(col("date"), "yyyyMM"))
    
    output_path = Config.CONFORMED_NORMALIZED_BASE_PATH + "/date_dim/date_dim_table"
    date_df.write.mode('overwrite').parquet(output_path)
    
    print(f"Date dimension table created and saved up to {end_date.strftime('%Y-%m-%d')}")
    

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("date_dim") \
        .getOrCreate()

if __name__ == "__main__":
    spark = create_spark_session()
    create_date_dimension(spark)