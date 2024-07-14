#!/bin/bash

directories=(
    "hdfs://localhost:9000/user/itversity/q-company_raw_layer"
    "hdfs://localhost:9000/user/itversity/q-company_standardized_layer"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/denormalized_model"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/denormalized_model/all_sales_fact_table"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/denormalized_model/offline_fact_table"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/denormalized_model/online_fact_table"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/branch_dim"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/customer_dim"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/product_dim"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/sales_agent_dim"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/date_dim"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/online_sales_fact"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer/normalized_model/offline_sales_fact"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events/views"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events/views/hourly_sales"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/hourly_sales"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events/views/category_performance"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/category_performance"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events/views/payment_method_trends"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/payment_method_trends"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events/views/customer_segmentation"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/customer_segmentation"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events/views/product_inventory"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/product_inventory"
)

for dir in "${directories[@]}"; do
    hdfs dfs -mkdir -p "$dir"
    if [ $? -eq 0 ]; then
        echo "Successfully created directory: $dir"
    else
        echo "Failed to create directory: $dir" >&2
    fi
done
