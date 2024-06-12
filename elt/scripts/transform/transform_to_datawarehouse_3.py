import sys
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col, explode

def process(parquet_file_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_times, dim_topics, dim_news, fact_news_topics, fact_news_companies)") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()

    # Connect to DuckDB
    database_path = '/home/anhcu/Project/Stock_project/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()
    
    # Step 1: Create DataFrame for dim_topics and insert new topics if they do not exist
    df_topics = df_spark.select(explode(col("topics")).alias("topic")) \
        .select("topic.topic").distinct().withColumnRenamed("topic", "topic_name")
    
    # Convert Spark DataFrame to Pandas DataFrame
    df_topics_pd = df_topics.toPandas()

    # Insert data into dim_topics
    conn.register('df_topics_pd', df_topics_pd)
    conn.execute("""
        INSERT INTO dim_topics (topic_name)
        SELECT * FROM df_topics_pd
        WHERE topic_name NOT IN (SELECT topic_name FROM dim_topics)
    """)
    print("Data inserted into dim_topics successfully!")
    
    # Step 2: Get yesterday's date and insert new time data into dim_time if it does not exist
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
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

    # Step 3: Create DataFrame for dim_news
    df_news = df_spark.select(
        col("title").alias("new_title"),
        col("url").alias("new_url"),
        col("time_published").alias("new_time_published"),
        col("authors").alias("new_authors"),
        col("summary").alias("new_summary"),
        col("source").alias("new_source"),
        col("overall_sentiment_score").alias("new_overall_sentiment_score"),
        col("overall_sentiment_label").alias("new_overall_sentiment_label")
    )
    
    # Add news_time_id to DataFrame
    df_news_pd = df_news.toPandas()
    df_news_pd["new_authors"] = df_news_pd["new_authors"].apply(str)
    df_news_pd['news_time_id'] = id_time_df['time_id'][0]
    print(df_news_pd)

    # Step 4: Insert DataFrame dim_news into dim_news table
    conn.register('df_news', df_news_pd)
    conn.execute('''
        INSERT INTO dim_news (
            new_title,
            new_url,
            new_time_published,
            new_authors,
            new_summary,
            new_source,
            new_overall_sentiment_score,
            new_overall_sentiment_label,
            news_time_id
        ) SELECT 
            new_title,
            new_url,
            new_time_published,
            new_authors,
            new_summary,
            new_source,
            new_overall_sentiment_score,
            new_overall_sentiment_label,
            news_time_id
        FROM df_news
    ''')
    print("Data inserted into dim_news successfully!")
    
    # Step 5: Create DataFrame for fact_news_topics
    df_fact_news_topics = df_spark.select(
        explode(col("topics")).alias("topic"),
        col("title").alias("new_title")
    ).select(
        col("topic.relevance_score").alias("new_topic_relevance_score"),
        col("topic.topic").alias("topic_name"),
        col("new_title")
    )
    df_fact_news_topics_pd = df_fact_news_topics.toPandas()
    print(df_fact_news_topics_pd)
    
    # Get corresponding topic_id from dim_topics
    id_topic_df = conn.execute('SELECT * FROM dim_topics').fetchdf()
    df_fact_news_topics_pd = df_fact_news_topics_pd.merge(id_topic_df, on='topic_name', how='left')
    df_fact_news_topics_pd = df_fact_news_topics_pd[df_fact_news_topics_pd['topic_id'].notnull()]
    
    # Get corresponding new_id from dim_news
    id_new_df = conn.execute('SELECT new_id, new_title FROM dim_news').fetchdf()
    df_fact_news_topics_pd = df_fact_news_topics_pd.merge(id_new_df, on='new_title', how='left')
    df_fact_news_topics_pd = df_fact_news_topics_pd[df_fact_news_topics_pd['new_id'].notnull()]
    print(df_fact_news_topics_pd)

    # Load DataFrame into fact_news_topics table
    conn.register('df_fact_news_topics_pd', df_fact_news_topics_pd)
    conn.execute('''
        INSERT INTO fact_news_topics (
            new_topic_new_id,
            new_topic_topic_id,
            new_topic_relevance_score
        ) SELECT 
            new_id,
            topic_id,
            new_topic_relevance_score
        FROM df_fact_news_topics_pd
    ''')
    print("Data inserted into fact_news_topics successfully!")
    
    # Step 6: Create DataFrame for fact_news_companies
    df_fact_news_companies = df_spark.select(
        explode(col("ticker_sentiment")).alias("ticker_sentiment"),
        col("title").alias("new_title")
    ).select(
        col("ticker_sentiment.relevance_score").alias("new_company_relevance_score"),
        col("ticker_sentiment.ticker").alias("company_ticket"),
        col("ticker_sentiment.ticker_sentiment_score").alias("new_company_ticker_sentiment_score"),
        col("ticker_sentiment.ticker_sentiment_label").alias("new_company_ticker_sentiment_label"),
        col("new_title")
    )
    df_fact_news_companies_pd = df_fact_news_companies.toPandas()
    print(df_fact_news_companies_pd)
    
    # Get corresponding new_id from dim_news
    df_fact_news_companies_pd = df_fact_news_companies_pd.merge(id_new_df, on='new_title', how='left')
    df_fact_news_companies_pd = df_fact_news_companies_pd[df_fact_news_companies_pd['new_id'].notnull()]
    
    # Get corresponding company_id from dim_companies
    query = """
        SELECT company_id, company_ticket, company_time_stamp
        FROM (
            SELECT 
                company_id, 
                company_ticket, 
                company_time_stamp,
                ROW_NUMBER() OVER (PARTITION BY company_ticket ORDER BY company_time_stamp DESC) as row_num
            FROM dim_companies
        ) subquery
        WHERE row_num = 1;
        """
    id_company_df = conn.execute(query).fetchdf()
    df_fact_news_companies_pd = df_fact_news_companies_pd.merge(id_company_df, on='company_ticket', how='left')
    df_fact_news_companies_pd = df_fact_news_companies_pd[df_fact_news_companies_pd['company_id'].notnull()]
    print(df_fact_news_companies_pd)
    
    # Load DataFrame into fact_news_companies table
    conn.register('df_fact_news_companies_pd', df_fact_news_companies_pd)
    conn.execute('''
        INSERT INTO fact_news_companies (
            new_company_company_id,
            new_company_new_id,
            new_company_relevance_score,
            new_company_ticker_sentiment_score,
            new_company_ticker_sentiment_label
        ) SELECT 
            company_id,
            new_id,
            new_company_relevance_score,
            new_company_ticker_sentiment_score,
            new_company_ticker_sentiment_label
        FROM df_fact_news_companies_pd
    ''')
    print("Data inserted into fact_news_companies successfully!")
    
    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    parquet_file_path = sys.argv[1]
    process(parquet_file_path)