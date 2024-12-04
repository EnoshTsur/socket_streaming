from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col


HDFS_URL = 'hdfs://192.168.1.2:8020'

spark_client: SparkSession = (
    SparkSession
        .builder
        .appName("socket_app")
        .master("local[3]")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.sql.legacy.timeParserPolicy","LEGACY")
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") 
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .getOrCreate()
)

# data_schema = StructType(fields=[
#     StructField("name", StringType()),
#     StructField("email", StringType()),
#     StructField("age", IntegerType()),
#     StructField("city", StringType()),
#     StructField("job", StringType())
# ])

# streamingDF = (
#     spark_client.readStream
#     .format("socket")
#     .option("host", "localhost")
#     .option("port", 9999)
#     .load()
# )

# parsed_df = (
#     streamingDF
#         .select(from_json(col("value"), schema=data_schema).alias("user"))
#         .select("user.*")
# )

# query = (
#     parsed_df.writeStream
#         .outputMode("append")
#         .format("console")
#         .option("truncate", False)
#         .start()
# )

# query = (
#     parsed_df.writeStream
#         .outputMode("append")
#         .format("parquet")
#         .option("path", f"{HDFS_URL}/users")
#         .option("checkpointLocation", f"{HDFS_URL}/checkpoints")
#         .partitionBy("city")
#         .trigger(processingTime="1 minute")
#         .start()
# )

# query.awaitTermination()

spark_client.read.parquet(f"{HDFS_URL}/users").show()