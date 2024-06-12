import duckdb

# Kết nối với database
con = duckdb.connect(database='datawarehouse.duckdb')

con.sql("Select * from dim_time limit 10;").show()
# con.sql("Select * from dim_companies limit 10;").show()
con.sql('''
            SELECT company_id, company_name, company_time_stamp, company_ticket, 
                    company_is_delisted, company_exchange_name, company_industry_name, 
                    company_industry_sector, company_sic_industry, company_sic_sector 
            FROM dim_companies
        ''').show()
con.sql("Select * from dim_topics;").show()
# con.sql("Select * from dim_news limit 10;").show()
con.sql("""SELECT new_id, new_title, new_url, new_time_published, new_authors, new_source, 
                new_overall_sentiment_score, new_overall_sentiment_label 
            FROM dim_news""").show()
con.sql("Select * from fact_candles limit 10;").show()
con.sql("Select * from fact_news_companies limit 10;").show()
con.sql("Select * from fact_news_topics limit 10;").show()
