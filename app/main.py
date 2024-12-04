from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from app.settings.config import HDFS_URL
from app.spark.schema import schema
from app.spark.client import spark_client


streamingDF = (
    spark_client.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Parse JSON
parsed_df = (
    streamingDF
    .select(from_json(col("value"), ArrayType(schema)).alias("data"))
    .select(explode("data").alias("record"))
    .select("record.*")
    .groupBy("Disease Category")
    .agg(
        count("*").alias("total_cases"),
        avg("Prevalence Rate (%)").alias("avg_prevalence"),
        max("Mortality Rate (%)").alias("max_mortality")
    )
)

checkpoint_path = f"{HDFS_URL}/checkpoints/health_stats"
output_path = f"{HDFS_URL}/health_stats"

def write_batch(batch_df, batch_id):
    """
    Write a batch to HDFS with error handling and logging
    """
    try:
        if not batch_df.isEmpty():
            # Log the batch information
            print(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            # Write to HDFS
            (batch_df
             .coalesce(1)  # Optimize small writes
             .write
             .mode("append")
             .parquet(f"{HDFS_URL}/health_stats"))
            
            print(f"Successfully wrote batch {batch_id} to HDFS")
            
            # Optionally verify the write
            count = spark_client.read.parquet(f"{HDFS_URL}/health_stats").count()
            print(f"Total records in HDFS after batch: {count}")
            
    except Exception as e:
        print(f"Error writing batch {batch_id}: {str(e)}")
        raise  # Re-raise the exception to let Spark handle it

query = (
    parsed_df.writeStream
    .foreachBatch(write_batch)
    .outputMode("update")
    .option("checkpointLocation", f"{HDFS_URL}/checkpoints/health_stats")
    .start()
)

# query = (
#     parsed_df.writeStream
#         .outputMode("update")
#         .format("console")
#         .option("truncate", False)
#         .start()
# )


query.awaitTermination()

# spark_client.read.parquet(f"{HDFS_URL}/users").show()
