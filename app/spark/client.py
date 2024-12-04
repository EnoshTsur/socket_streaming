from pyspark.sql import SparkSession
from app.settings.config import HDFS_URL

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