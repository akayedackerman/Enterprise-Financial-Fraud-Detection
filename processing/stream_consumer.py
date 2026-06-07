import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the exact incoming JSON schema from your Kafka simulator
TRANSACTION_SCHEMA = StructType([
	StructField("timestamp", StringType(), True),
	StructField("user_id", StringType(), True),
	StructField("amount", DoubleType(), True),
	StructField("merchant", StringType(), True),
	StructField("location", StringType(), True)
])


def main():
	print("✨ Starting PySpark Fraud Detection Streaming Pipeline...")

	os.environ["HADOOP_USER_NAME"] = "root"

	# Build the Spark instance with the native Kafka compilation packages
	spark = SparkSession.builder \
		.appName("FraudDetection_StreamConsumer") \
		.master("local[*]") \
		.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
		.config("spark.driver.bindAddress", "127.0.0.1") \
		.config("spark.driver.host", "127.0.0.1") \
		.config("spark.bindAddress", "127.0.0.1") \
		.config("spark.driver.memory", "2g") \
		.getOrCreate()

	spark.sparkContext.setLogLevel("WARN")

	# 1. Connect to the Kafka Broker running inside Docker
	print("📥 Connecting to Kafka Broker stream at localhost:9092...")
	raw_kafka_df = spark.readStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "localhost:9092") \
		.option("subscribe", "financial_transactions") \
		.option("startingOffsets", "latest") \
		.load()

	# 2. Extract and cast the binary payload into structured strings
	string_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload")

	# 3. Parse JSON strings into structured columns according to our schema
	structured_df = string_df \
		.select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data")) \
		.select("data.*")

	# 4. Stream and Write the structured data to Hadoop HDFS as Parquet layers
	# HDFS NameNode runs at hdfs://localhost:8020 (forwarded via Docker network)
	hdfs_output_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/raw_transactions"
	checkpoint_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/checkpoints"

	print(f"💾 Directing Parquet write stream to HDFS storage lake: {hdfs_output_path}")

	query = structured_df.writeStream \
		.format("parquet") \
		.option("path", hdfs_output_path) \
		.option("checkpointLocation", checkpoint_path) \
		.outputMode("append") \
		.start()

	# Keep the streaming query active
	query.awaitTermination()


if __name__ == "__main__":
	main()