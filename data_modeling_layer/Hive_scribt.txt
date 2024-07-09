create database q_company_modeling;
use q_company_modeling;

CREATE EXTERNAL TABLE branches_dim (
    branch_id INT,
    branch_location STRING,
    branch_establish_date STRING,
    branch_class STRING,
    branch_key INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/branches_dim.csv/'
--------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE sales_agent_dim (
    sales_agent_id INT,
    sales_agent_hire_date DATE,
    sales_agent_name STRING,
    sales_agent_key INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/sales_agent_dim.csv/'
-------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE product_dim (
    product_id STRING,
    product_name STRING,
    product_category STRING,
    unit_price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/products_dim.csv/';
---------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE customer_dim (
    customer_id STRING,
    customer_name STRING,
    cleaned_email STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/customer_dim.csv/';
-----------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE date_dim (
    datee DATE,
    surrogate_key BIGINT,
    day INT,
    day_of_week INT,
    day_of_year INT,
    week_of_year INT,
    month INT,
    quarter INT,
    year INT,
    is_weekend INT,
    month_name STRING,
    day_name STRING,
    year_month STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/date_dim.csv/';
------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE offline_transactions_fact(
    transaction_id STRING,
    units INT,
    discount DOUBLE,
    payment_method STRING,
    total_price DOUBLE,
    surrogate_key BIGINT,
    product_key STRING,
    customer_key STRING,
    branch_key STRING,
    sales_agent_key STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/offline_transactions_fact.csv/';
-------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE online_transactions_fact (
    transaction_id STRING,
    units INT,
    discount DOUBLE,
    payment_method STRING,
    group_from INT,
    total_price DOUBLE,
    surrogate_key BIGINT,
    shipping_zip_code STRING,
    shipping_state STRING,
    shipping_city STRING,
    shipping_street_name STRING,
    product_key STRING,
    customer_key STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Graduation_Project/transformed_data/schema/online_transactions_fact.csv/';
