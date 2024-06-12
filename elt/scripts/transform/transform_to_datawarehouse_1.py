import sys
import duckdb
import pandas as pd
from pyspark.sql import SparkSession

def process(parquet_file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_companies)") \
        .getOrCreate()

    # Read Parquet file into PySpark DataFrame
    df_spark = spark.read.parquet(parquet_file_path)

    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()

    # Convert PySpark DataFrame to Pandas DataFrame
    df_pandas = df_spark.toPandas()

    # Path to DuckDB database file
    database_path = '/home/anhcu/Project/Stock_project/datawarehouse.duckdb'

    # Connect to DuckDB
    conn = duckdb.connect(database=database_path)

    # Register the Pandas DataFrame as a DuckDB table and insert data into dim_companies
    conn.register('df_pandas', df_pandas)
    conn.execute('''
        INSERT INTO dim_companies (
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        ) SELECT 
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        FROM df_pandas
    ''')

    # Close DuckDB connection
    conn.close()

    # Stop Spark session
    spark.stop()

    print("Data has been successfully inserted into dim_companies in DuckDB!")

if __name__ == "__main__":
    # Get the Parquet file path from command-line arguments
    parquet_file_path = sys.argv[1]
    # Process the Parquet file and insert data into DuckDB
    process(parquet_file_path)