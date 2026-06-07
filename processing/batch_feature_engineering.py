import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def main():
	print("🛠️ Starting Distributed Batch Feature Engineering Job...")

	# Authenticate as HDFS superuser
	os.environ["HADOOP_USER_NAME"] = "root"

	# Initialize a Batch Spark Session targeting loopback interfaces
	spark = SparkSession.builder \
		.appName("FraudDetection_BatchFeatureEngineering") \
		.master("local[*]") \
		.config("spark.driver.bindAddress", "127.0.0.1") \
		.config("spark.driver.host", "127.0.0.1") \
		.config("spark.bindAddress", "127.0.0.1") \
		.config("spark.driver.memory", "4g") \
		.getOrCreate()

	spark.sparkContext.setLogLevel("WARN")

	# 1. Ingest historical Parquet logs from the Hadoop cluster
	hdfs_input_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/raw_transactions"
	print(f"📖 Reading raw transactional files from HDFS storage: {hdfs_input_path}")
	raw_df = spark.read.parquet(hdfs_input_path)

	# Added F. prefix to col() and updated pattern to "yyyy-MM-dd HH:mm:ss"
	df_with_time = raw_df.withColumn("tx_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

	# 2. Define Analytical Windows based on User Profiles
	# Cast timestamp into long integer to calculate time-range bounds (in seconds)
	time_secs = F.col("tx_timestamp").cast("long")

	# 1 Hour Window = 3600 seconds trailing
	window_1h = Window.partitionBy("user_id").orderBy(time_secs).rangeBetween(-3600, 0)
	# 30 Day Window = 2592000 seconds trailing
	window_30d = Window.partitionBy("user_id").orderBy(time_secs).rangeBetween(-2592000, 0)

	print("⚡ Extracting velocity windows and behavior metrics...")

	# 3. Compute Profile Aggregations
	enriched_df = df_with_time \
		.withColumn("user_tx_count_1h", F.count("amount").over(window_1h)) \
		.withColumn("user_avg_amount_30d", F.round(F.avg("amount").over(window_30d), 2))

	# 4. Deriving Anomaly Indicators
	# Flag transactions that are 3x greater than the user's running 30-day average
	features_df = enriched_df.withColumn(
		"is_amount_anomaly",
		F.when(F.col("amount") > (F.col("user_avg_amount_30d") * 3), 1).otherwise(0)
	)

	# 5. Export calculated vectors back to HDFS as an ML-ready analytical tier
	hdfs_output_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/engineered_features"
	print(f"💾 Writing enriched feature matrices back to HDFS: {hdfs_output_path}")

	features_df.write \
		.mode("overwrite") \
		.parquet(hdfs_output_path)

	print("✅ Feature engineering calculation completed successfully!")

	# Showcase a snippet of our engineered dataset vectors
	features_df.select("timestamp", "user_id", "amount", "user_tx_count_1h", "user_avg_amount_30d",
	                   "is_amount_anomaly").show(10, truncate=False)

	spark.stop()


if __name__ == "__main__":
	main()