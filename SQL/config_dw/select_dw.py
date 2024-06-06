import duckdb

# Kết nối với database
con = duckdb.connect(database='datawarehouse.duckdb')

con.sql("Select * from dim_time limit 10;").show()
con.sql("Select * from dim_companies limit 10;").show()
con.sql("Select * from dim_topics;").show()
con.sql("Select * from dim_news limit 10;").show()
con.sql("Select * from fact_candles limit 10;").show()
con.sql("Select * from fact_news_companies limit 10;").show()
con.sql("Select * from fact_news_topics limit 10;").show()
