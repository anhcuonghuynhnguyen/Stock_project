import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import duckdb
import pandas as pd
from datetime import datetime, timedelta

# Create SparkSession
spark = SparkSession.builder \
    .appName("Insert Parquet into DuckDB (dim_times, dim_topics, dim_news, fact_news_topics, fact_news_companies)") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

# Path to Parquet file
parquet_file_path = "datalake/news/crawl_news_06_03_2024_13_00_10_06_03_2024_14_22_26.parquet"

# Connect to DuckDB
conn = duckdb.connect("datawarehouse.duckdb")

# Read Parquet file into Spark DataFrame
df_pandas = spark.read.parquet(parquet_file_path)

# Display schema and a few rows of data
df_pandas.printSchema()
df_pandas.show()

# Step 1: Create DataFrame for dim_topics and insert new topics if they do not exist
df_topics = df_pandas.select(explode(col("topics")).alias("topic")) \
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
df_news = df_pandas.select(
    col("title").alias("new_title"),
    col("time_published").alias("new_time_published"),
    col("authors").alias("new_authors"),
    col("summary").alias("new_summary"),
    col("source").alias("new_source"),
    col("source_domain").alias("new_source_domain"),
    col("overall_sentiment_score").alias("new_overall_sentiment_score"),
    col("overall_sentiment_label").alias("new_overall_sentiment_label")
)

# Add news_time_id to DataFrame
df_news = df_news.toPandas()
df_news["new_authors"] = df_news["new_authors"].apply(str)
df_news['news_time_id'] = id_time_df['time_id'][0]
print(df_news)

# Step 4: Insert DataFrame dim_news into dim_news table
conn.register('df_news', df_news)
conn.execute('''
    INSERT INTO dim_news (
        new_title,
        new_time_published,
        new_authors,
        new_summary,
        new_source,
        new_source_domain,
        new_overall_sentiment_score,
        new_overall_sentiment_label,
        news_time_id
    ) SELECT 
        new_title,
        new_time_published,
        new_authors,
        new_summary,
        new_source,
        new_source_domain,
        new_overall_sentiment_score,
        new_overall_sentiment_label,
        news_time_id
    FROM df_news
''')
print("Data inserted into dim_news successfully!")

# Step 5: Create DataFrame for fact_news_topics
df_fact_news_topics = df_pandas.select(
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
id_topic_df = conn.execute(f'''
    SELECT * FROM dim_topics
''').fetchdf()
df_fact_news_topics_pd = df_fact_news_topics_pd.merge(id_topic_df, on='topic_name', how='left')
df_fact_news_topics_pd = df_fact_news_topics_pd[df_fact_news_topics_pd['topic_id'].notnull()]

# Get corresponding new_id from dim_news
id_new_df = conn.execute(f'''
    SELECT new_id, new_title FROM dim_news
''').fetchdf()
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
df_fact_news_companies = df_pandas.select(
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
id_company_df = conn.execute(f'''
    SELECT company_id, company_ticket FROM dim_companies
''').fetchdf()
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
