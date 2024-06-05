#!/bin/bash

# Mảng các thư mục local và các thư mục HDFS tương ứng
declare -A directories=(
    ["etl/data/completed/load_api_news_to_dl"]="/user/anhcu/datalake/news"
    ["etl/data/completed/load_api_ohlcs_to_dl"]="/user/anhcu/datalake/ohlcs"
    ["etl/data/completed/load_db_to_dl"]="/user/anhcu/datalake/companies"
)

# Lặp qua các cặp thư mục local và HDFS
for local_directory in "${!directories[@]}"; do
    hdfs_directory="${directories[$local_directory]}"

    # Tìm file Parquet mới nhất
    latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

    # Kiểm tra nếu có file
    if [ -z "$latest_file" ]; then
        echo "No Parquet file found in the directory $local_directory."
    else
        # Tải file lên HDFS
        hdfs dfs -put "$latest_file" "$hdfs_directory"
        echo "Uploaded $latest_file to $hdfs_directory on HDFS"
    fi
done
