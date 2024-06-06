import duckdb

# Kết nối với database
con = duckdb.connect(database='datawarehouse.duckdb')

con.sql("DESCRIBE dim_time;").show()
con.sql("DESCRIBE dim_companies;").show()
con.sql("DESCRIBE dim_topics;").show()
con.sql("DESCRIBE dim_news;").show()
con.sql("DESCRIBE fact_candles;").show()
con.sql("DESCRIBE fact_news_companies;").show()
con.sql("DESCRIBE fact_news_topics;").show()
