from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("CaseSensitiveExample") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

# Đường dẫn đến file Parquet
parquet_file_path = "datalake/news/crawl_news_06_03_2024_13_00_10_06_03_2024_14_22_26.parquet"

# Định nghĩa schema phù hợp với schema thực tế của file Parquet
# schema = StructType([
#     StructField("T", StringType()),
#     StructField("v", DoubleType()),
#     StructField("vw", DoubleType()),
#     StructField("o", DoubleType()),
#     StructField("c", DoubleType()),
#     StructField("h", DoubleType()),
#     StructField("l", DoubleType()),
#     StructField("t", LongType()),
#     StructField("n", DoubleType()),
#     StructField("otc", BooleanType())
# ])
# Đọc file Parquet với schema đã xác định
# df = spark.read.schema(schema).parquet(parquet_file_path)
df = spark.read.parquet(parquet_file_path)
# df = spark.read.parquet(parquet_file_path)

# Đổi tên các cột
# df = df.withColumnRenamed("T", "ticket") \
#     .withColumnRenamed("v", "volume") \
#     .withColumnRenamed("vw", "volume_weighted") \
#     .withColumnRenamed("o", "open") \
#     .withColumnRenamed("c", "close") \
#     .withColumnRenamed("h", "high") \
#     .withColumnRenamed("l", "low") \
#     .withColumnRenamed("t", "time_stamp") \
#     .withColumnRenamed("n", "num_of_trades") \
#     .withColumnRenamed("otc", "is_otc")

# Hiển thị schema
df.printSchema()

# Hiển thị một vài dòng dữ liệu
df.show()
