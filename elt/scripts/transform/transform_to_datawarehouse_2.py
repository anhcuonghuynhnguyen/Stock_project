import sys
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

def process(parquet_file_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_times, fact_candles)") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()  
    
    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Rename columns for clarity
    df_spark = df_spark.withColumnRenamed("T", "company_ticket") \
        .withColumnRenamed("v", "volume") \
        .withColumnRenamed("vw", "volume_weighted") \
        .withColumnRenamed("o", "open") \
        .withColumnRenamed("c", "close") \
        .withColumnRenamed("h", "high") \
        .withColumnRenamed("l", "low") \
        .withColumnRenamed("t", "time_stamp") \
        .withColumnRenamed("n", "num_of_trades") \
        .withColumnRenamed("otc", "is_otc")
    
    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()
    
    # Convert Spark DataFrame to Pandas DataFrame
    df_pandas = df_spark.toPandas()
    print(df_pandas)
    
    # Get yesterday's date
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    
    # Connect to DuckDB
    database_path = '/home/anhcu/Project/Stock_project/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    # Insert new time data into dim_time if it does not exist
    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        SELECT
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{yesterday}'
        )
    ''')
    
    # Get corresponding time_id from dim_time
    id_time_df = conn.execute(f'''
        SELECT time_id FROM dim_time WHERE date = '{yesterday}'
    ''').fetchdf()
    
    # Get corresponding company_id from dim_companies
    id_company_df = conn.execute(f'''
        SELECT company_id, company_ticket FROM dim_companies
    ''').fetchdf()
    print(id_company_df)
    
    # Create a new DataFrame containing company_id and company_ticket from dim_companies
    companies_df = id_company_df.drop_duplicates(subset=['company_ticket'], keep='last')
    print(companies_df)
    
    # Join companies_df to df_pandas to get corresponding company_id
    df_pandas = df_pandas.merge(companies_df, on='company_ticket', how='left')
    df_pandas = df_pandas[df_pandas['company_id'].notnull()]
    print(df_pandas)
    
    # Add candles_time_id to DataFrame
    df_pandas['candles_time_id'] = id_time_df['time_id'][0]
    print(df_pandas)
    
    # Load DataFrame into fact_candles table
    conn.register('df_pandas', df_pandas)
    conn.execute('''
        INSERT INTO fact_candles (
            candle_volume,
            candle_volume_weighted,
            candle_open,
            candle_close,
            candle_high,
            candle_low,
            candle_time_stamp,
            candle_num_of_trades,
            candle_is_otc,
            candles_time_id,
            candle_company_id
        ) SELECT 
            volume,
            volume_weighted,
            open,
            close,
            high,
            low,
            time_stamp,
            num_of_trades,
            is_otc,
            candles_time_id,
            company_id
        FROM df_pandas
    ''')
    print("Data inserted into fact_candles successfully!")
    
    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    # Get the Parquet file path from command-line arguments
    parquet_file_path = sys.argv[1]
    # Process the Parquet file and insert data into DuckDB
    process(parquet_file_path)