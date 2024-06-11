#!/bin/bash

# Mảng các thư mục local và các thư mục HDFS tương ứng
# declare -A directories=(
#     ["/home/anhcu/Project/Stock_project/elt/data/completed/load_api_ohlcs_to_dl"]="/user/anhcu/datalake/ohlcs"
#     ["/home/anhcu/Project/Stock_project/elt/data/completed/load_db_to_dl"]="/user/anhcu/datalake/companies"
#     ["/home/anhcu/Project/Stock_project/elt/data/completed/load_api_news_to_dl"]="/user/anhcu/datalake/news"
# )

# Biến để lưu tên tệp mới nhất
latest_files=""

local_directory=/home/anhcu/Project/Stock_project/elt/data/completed/load_db_to_dl
hdfs_directory=/user/anhcu/datalake/companies
# hdfs_directory="${directories[$local_directory]}"

# Tìm file Parquet mới nhất
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Kiểm tra nếu có file
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Tải file lên HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

# Thêm tên tệp và thư mục HDFS tương ứng vào biến
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# Lặp qua các cặp thư mục local và HDFS
# for local_directory in "${!directories[@]}"; do
local_directory=/home/anhcu/Project/Stock_project/elt/data/completed/load_api_ohlcs_to_dl
hdfs_directory=/user/anhcu/datalake/ohlcs
# hdfs_directory="${directories[$local_directory]}"

# Tìm file Parquet mới nhất
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Kiểm tra nếu có file
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Tải file lên HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

# Thêm tên tệp và thư mục HDFS tương ứng vào biến
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi
# done

local_directory=/home/anhcu/Project/Stock_project/elt/data/completed/load_api_news_to_dl
hdfs_directory=/user/anhcu/datalake/news
# hdfs_directory="${directories[$local_directory]}"

# Tìm file Parquet mới nhất
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Kiểm tra nếu có file
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Tải file lên HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

# Thêm tên tệp và thư mục HDFS tương ứng vào biến
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# In tên tệp mới nhất (cho Airflow XCom)
echo -e "$latest_files"