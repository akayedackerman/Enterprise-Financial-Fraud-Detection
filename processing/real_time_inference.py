import os
import pickle
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Match the baseline incoming Kafka JSON schema
TRANSACTION_SCHEMA = StructType([
	StructField("timestamp", StringType(), True),
	StructField("user_id", StringType(), True),
	StructField("amount", DoubleType(), True),
	StructField("merchant", StringType(), True),
	StructField("location", StringType(), True)
])


def main():
	print("🚀 Initializing Live Real-Time XGBoost Inference Firewall Engine...")

	# Load the pre-trained Path B XGBoost model binary
	model_path = "models/fraud_xgboost_model.pkl"
	if not os.path.exists(model_path):
		print(f"❌ Critical Error: Pre-trained model binary not found at {model_path}. Run training first.")
		return

	with open(model_path, "rb") as model_file:
		xgb_model = pickle.load(model_file)
	print("🧠 Pre-trained XGBoost Model Binary loaded into memory successfully.")

	# Initialize localized loopback Spark streaming instance
	spark = SparkSession.builder \
		.appName("FraudDetection_RealTimeInference") \
		.master("local[*]") \
		.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
		.config("spark.driver.bindAddress", "127.0.0.1") \
		.config("spark.driver.host", "127.0.0.1") \
		.config("spark.bindAddress", "127.0.0.1") \
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	# Connect to the live transactional Kafka cluster broker
	raw_kafka_df = spark.readStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "localhost:9092") \
		.option("subscribe", "financial_transactions") \
		.load()

	# Parse JSON rows into structured streaming objects
	structured_df = raw_kafka_df \
		.selectExpr("CAST(value AS STRING) as json_payload") \
		.select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data")) \
		.select("data.*")

	# Define our micro-batch processing loop to execute model predictions on the fly
	def evaluate_batch_prediction(batch_df, batch_id):
		if batch_df.count() == 0:
			return

		# Convert streaming micro-batch data to Pandas for local model evaluation
		pdf = batch_df.toPandas()

		# Generate rolling mock state indicators for streaming verification
		# (Simulating real-time production feature store lookups)
		pdf["user_avg_amount_30d"] = pdf["amount"].apply(lambda x: round(x * 0.95, 2))
		pdf["user_tx_count_1h"] = pdf.index + 1  # Incremental mock velocity trigger

		# Extract features matching model input specifications
		feature_matrix = pdf[["amount", "user_tx_count_1h", "user_avg_amount_30d"]]

		# Compute probabilities and final predictions
		probabilities = xgb_model.predict_proba(feature_matrix)[:, 1]
		predictions = xgb_model.predict(feature_matrix)

		pdf["fraud_probability"] = probabilities
		pdf["is_fraud"] = predictions

		# Filter out flagged events and print immediate security indicators
		for _, row in pdf.iterrows():
			prob_percent = round(row['fraud_probability'] * 100, 2)
			if row["is_fraud"] == 1 or row["fraud_probability"] > 0.5:
				print(
					f"🚨 ALERT: HIGH FRAUD RISK FOR {row['user_id']} | Amount: ${row['amount']} | Merchant: {row['merchant']} | Risk Prob: {prob_percent}%")
			else:
				print(
					f"✅ Clean Transaction: {row['user_id']} | Amount: ${row['amount']} | Merchant: {row['merchant']} | Risk Prob: {prob_percent}%")

	# Start the continuous prediction evaluation pipeline loop
	print("👀 Active monitoring started. Analyzing incoming Kafka events live...")
	query = structured_df.writeStream \
		.foreachBatch(evaluate_batch_prediction) \
		.start()

	query.awaitTermination()


if __name__ == "__main__":
	main()