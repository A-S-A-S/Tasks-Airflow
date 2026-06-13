from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum
import sys
import os


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("CoinGeckoParquetProcessor") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print(f"Reading from: {input_path}")
    df = spark.read.parquet(input_path)
    
    df.printSchema()
    print(f"Total records: {df.count()}")

    #just a simple aggregation
    summary = df.groupBy("subset").agg(
        count("*").alias("coin_count"),
        avg("current_price").alias("avg_price_usd"),
        spark_sum("market_cap").alias("total_market_cap"),
        avg("price_change_pct_24h").alias("avg_24h_change")
    )
    summary.show(truncate=False)

    print(f"Writing summary to: {output_path}")
    summary.write.mode("overwrite").parquet(output_path)

    spark.stop()
    print("Spark job completed successfully")