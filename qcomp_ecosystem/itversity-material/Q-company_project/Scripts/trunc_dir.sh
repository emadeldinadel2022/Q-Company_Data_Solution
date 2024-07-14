#!/bin/bash

directories=(
    "hdfs://localhost:9000/user/itversity/q-company_raw_layer"
    "hdfs://localhost:9000/user/itversity/q-company_standardized_layer"
    "hdfs://localhost:9000/user/itversity/q-company_conformed_layer"
    "hdfs://localhost:9000/user/itversity/q-company_customer_events"
    "hdfs://localhost:9000/user/itversity/checkpoints_custevents/"
)

for dir in "${directories[@]}"; do
    hdfs dfs -rm -r "$dir"
    if [ $? -eq 0 ]; then
        echo "Successfully deleted directory: $dir"
    else
        echo "Failed to delete directory: $dir" >&2
    fi
done
