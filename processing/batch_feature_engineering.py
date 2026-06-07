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

	# 4. Deriving Realistic Behavioral Fraud Indicators (Path B)
	# Fraud is signaled by combining high 1-hour velocity with an elevated purchase amount,
	# combined with a small random hash modifier to prevent a perfect mathematical split.
	features_df = enriched_df.withColumn(
		"is_fraudulent_claim",
		F.when(
			(F.col("user_tx_count_1h") > 3) &
			(F.col("amount") > (F.col("user_avg_amount_30d") * 1.5)) &
			(F.rand(seed=42) > 0.15),  # 85% structured pattern
			1
		).otherwise(
			F.when((F.col("amount") > 3000) & (F.rand(seed=42) > 0.4), 1).otherwise(0)  # High-value outliers
		)
	)

	# 5. Export calculated vectors back to HDFS (Update path target label column)
	# Change the preview and selection to show our new target field: "is_fraudulent_claim"
	hdfs_output_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/engineered_features"
	print(f"💾 Writing enriched feature matrices back to HDFS: {hdfs_output_path}")

	features_df.write \
		.mode("overwrite") \
		.parquet(hdfs_output_path)

	print("✅ Feature engineering calculation completed successfully!")
	features_df.select("timestamp", "user_id", "amount", "user_tx_count_1h", "user_avg_amount_30d",
	                   "is_fraudulent_claim").show(10, truncate=False)

	spark.stop()


if __name__ == "__main__":
	main()