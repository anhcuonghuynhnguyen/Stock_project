CREATE DATABASE Datawarehouse;

CREATE TABLE IF NOT EXISTS Dim_Companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(100),
    company_time_stamp TIMESTAMP,
    company_ticket VARCHAR(10) NOT NULL,
    company_is_delisted BOOLEAN NOT NULL,
    company_category VARCHAR(100) NOT NULL,
    company_currency VARCHAR(10) NOT NULL,
    company_location VARCHAR(100) NOT NULL,
    company_exchange_name VARCHAR(10) NOT NULL,
    company_region_name VARCHAR(50) NOT NULL,
    company_region_local_open TIME NOT NULL,
    company_region_local_close TIME NOT NULL,
    industry_name VARCHAR(100) NOT NULL,
    industry_sector VARCHAR(100) NOT NULL,
    sic_industry VARCHAR(100) NOT NULL,
    sic_sector VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Times (
    time_id INT PRIMARY KEY,
    time_day_of_week INT NOT NULL,
    time_day INT NOT NULL,
    time_month INT NOT NULL,
    time_quarter INT NOT NULL,
    time_year INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Topics (
    topic_id INT PRIMARY KEY,
    topic_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_News (
    new_id INT PRIMARY KEY,
    new_title VARCHAR(100) NOT NULL,
    new_time_published CHAR(15) NOT NULL,
    new_author VARCHAR(100) NOT NULL,
    new_summary TEXT NOT NULL,
    new_source VARCHAR(100) NOT NULL,
    new_source_domain VARCHAR(100) NOT NULL,
    new_overall_sentiment_score DECIMAL NOT NULL,
    new_overall_sentiment_label VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Fact_Candles (
    candle_id INT PRIMARY KEY,
    candle_company_id INT NOT NULL,
    candle_time_id INT NOT NULL,
    candle_open DECIMAL NOT NULL,
    candle_high DECIMAL NOT NULL,
    candle_low DECIMAL NOT NULL,
    candle_close DECIMAL NOT NULL,
    candle_volume INT NOT NULL,
    CONSTRAINT fk_candle_company_id
        FOREIGN KEY(candle_company_id)
        REFERENCES Dim_Companies(company_id),
    CONSTRAINT fk_candle_time_id
        FOREIGN KEY(candle_time_id)
        REFERENCES Dim_Times(time_id)
);

CREATE TABLE IF NOT EXISTS Fact_News_Companies (
    new_company_id INT PRIMARY KEY,
    new_company_company_id INT NOT NULL,
    new_company_new_id INT NOT NULL,
    new_company_relevance_score DECIMAL NOT NULL,
    new_company_new_ticker_sentiment_score DECIMAL NOT NULL,
    new_company_new_ticker_sentiment_label VARCHAR(100) NOT NULL,
    CONSTRAINT fk_new_company_company_id
        FOREIGN KEY(new_company_company_id)
        REFERENCES Dim_Companies(company_id),
    CONSTRAINT fk_new_company_new_id
        FOREIGN KEY(new_company_new_id)
        REFERENCES Dim_News(new_id)
);

CREATE TABLE IF NOT EXISTS Fact_News_Topics (
    new_topic_id INT PRIMARY KEY,
    new_topic_new_id INT NOT NULL,
    new_topic_topic_id INT NOT NULL,
    new_topic_relevance_score DECIMAL NOT NULL,
    CONSTRAINT fk_new_topic_new_id
        FOREIGN KEY(new_topic_new_id)
        REFERENCES Dim_News(new_id),
    CONSTRAINT fk_new_topic_topic_id
        FOREIGN KEY(new_topic_topic_id)
        REFERENCES Dim_Topics(topic_id)
);