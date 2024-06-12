-- Tạo SEQUENCE cho các bảng
CREATE SEQUENCE company_id_seq;
CREATE SEQUENCE topic_id_seq;
CREATE SEQUENCE new_id_seq;
CREATE SEQUENCE candle_id_seq;
CREATE SEQUENCE new_company_id_seq;
CREATE SEQUENCE new_topic_id_seq;
CREATE SEQUENCE time_id_seq;

-- dim_time table
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER DEFAULT NEXTVAL('time_id_seq') PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month VARCHAR(10),
    quarter VARCHAR(10),
    year INTEGER
);

-- dim_companies table
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id INTEGER DEFAULT NEXTVAL('company_id_seq') PRIMARY KEY,
    company_name VARCHAR(255),
    company_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    company_ticket VARCHAR(10) NOT NULL,
    company_is_delisted BOOLEAN NOT NULL,
    company_category VARCHAR(255),
    company_currency VARCHAR(10) NOT NULL,
    company_location VARCHAR(255),
    company_exchange_name VARCHAR(100) NOT NULL,
    company_region_name VARCHAR(50) NOT NULL,
    company_industry_name VARCHAR(255),
    company_industry_sector VARCHAR(255),
    company_sic_industry VARCHAR(255),
    company_sic_sector VARCHAR(255),
);

-- dim_topics table
CREATE TABLE IF NOT EXISTS dim_topics (
    topic_id INTEGER DEFAULT NEXTVAL('topic_id_seq') PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL,
    CONSTRAINT unique_topic UNIQUE (topic_name)
);

-- dim_news table
CREATE TABLE IF NOT EXISTS dim_news (
    new_id INTEGER DEFAULT NEXTVAL('new_id_seq') PRIMARY KEY,
    new_title TEXT NOT NULL,
    new_url TEXT NOT NULL,
    new_time_published CHAR(15) NOT NULL,
    new_authors VARCHAR[],
    new_summary TEXT,
    new_source TEXT,
    new_overall_sentiment_score DOUBLE NOT NULL,
    new_overall_sentiment_label VARCHAR(255) NOT NULL,
    news_time_id INTEGER,
    FOREIGN KEY (news_time_id) REFERENCES dim_time(time_id)
);

-- fact_candles table
CREATE TABLE IF NOT EXISTS fact_candles (
    candle_id INTEGER DEFAULT NEXTVAL('candle_id_seq') PRIMARY KEY,
    candle_company_id INTEGER NOT NULL,
    candle_volume INTEGER NOT NULL,
    candle_volume_weighted DOUBLE NOT NULL,
    candle_open DOUBLE NOT NULL,
    candle_close DOUBLE NOT NULL,
    candle_high DOUBLE NOT NULL,
    candle_low DOUBLE NOT NULL,
    candle_time_stamp CHAR(15) NOT NULL,
    candle_num_of_trades INTEGER NOT NULL,
    candle_is_otc BOOLEAN DEFAULT false,
    candles_time_id INTEGER,
    FOREIGN KEY (candle_company_id) REFERENCES dim_companies(company_id),
    FOREIGN KEY (candles_time_id) REFERENCES dim_time(time_id)
);

-- fact_news_companies table
CREATE TABLE IF NOT EXISTS fact_news_companies (
    new_company_id INTEGER DEFAULT NEXTVAL('new_company_id_seq') PRIMARY KEY,
    new_company_company_id INTEGER NOT NULL,
    new_company_new_id INTEGER NOT NULL,
    new_company_relevance_score DOUBLE NOT NULL,
    new_company_ticker_sentiment_score DOUBLE NOT NULL,
    new_company_ticker_sentiment_label VARCHAR(100) NOT NULL,
    FOREIGN KEY (new_company_company_id) REFERENCES dim_companies(company_id),
    FOREIGN KEY (new_company_new_id) REFERENCES dim_news(new_id)
);

-- fact_news_topics table
CREATE TABLE IF NOT EXISTS fact_news_topics (
    new_topic_id INTEGER DEFAULT NEXTVAL('new_topic_id_seq') PRIMARY KEY,
    new_topic_new_id INTEGER NOT NULL,
    new_topic_topic_id INTEGER NOT NULL,
    new_topic_relevance_score DOUBLE NOT NULL,
    FOREIGN KEY (new_topic_new_id) REFERENCES dim_news(new_id),
    FOREIGN KEY (new_topic_topic_id) REFERENCES dim_topics(topic_id)
);
