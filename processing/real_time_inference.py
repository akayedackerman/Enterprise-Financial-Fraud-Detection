import os
import pickle
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from kafka import KafkaProducer  # Imported for broadcasting alerts

TRANSACTION_SCHEMA = StructType([
	StructField("timestamp", StringType(), True),
	StructField("user_id", StringType(), True),
	StructField("amount", DoubleType(), True),
	StructField("merchant", StringType(), True),
	StructField("location", StringType(), True)
])


def main():
	print("🚀 Initializing Live Real-Time XGBoost Inference Firewall Engine...")

	model_path = "models/fraud_xgboost_model.pkl"
	if not os.path.exists(model_path):
		print(f"❌ Critical Error: Pre-trained model binary not found at {model_path}.")
		return

	with open(model_path, "rb") as model_file:
		xgb_model = pickle.load(model_file)
	print("🧠 Pre-trained XGBoost Model Binary loaded into memory successfully.")

	# Initialize standard Kafka Producer for alert streaming notifications
	alert_producer = KafkaProducer(
		bootstrap_servers=['localhost:9092'],
		value_serializer=lambda v: json.dumps(v).encode('utf-8')
	)

	spark = SparkSession.builder \
		.appName("FraudDetection_RealTimeInference") \
		.master("local[*]") \
		.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
		.config("spark.driver.bindAddress", "127.0.0.1") \
		.config("spark.driver.host", "127.0.0.1") \
		.config("spark.bindAddress", "127.0.0.1") \
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	raw_kafka_df = spark.readStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "localhost:9092") \
		.option("subscribe", "financial_transactions") \
		.load()

	structured_df = raw_kafka_df \
		.selectExpr("CAST(value AS STRING) as json_payload") \
		.select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data")) \
		.select("data.*")

	def evaluate_batch_prediction(batch_df, batch_id):
		if batch_df.count() == 0:
			return

		pdf = batch_df.toPandas()
		pdf["user_avg_amount_30d"] = pdf["amount"].apply(lambda x: round(x * 0.95, 2))
		pdf["user_tx_count_1h"] = pdf.index + 1

		feature_matrix = pdf[["amount", "user_tx_count_1h", "user_avg_amount_30d"]]
		probabilities = xgb_model.predict_proba(feature_matrix)[:, 1]
		predictions = xgb_model.predict(feature_matrix)

		pdf["fraud_probability"] = probabilities
		pdf["is_fraud"] = predictions

		for _, row in pdf.iterrows():
			prob_percent = round(row['fraud_probability'] * 100, 2)

			# Construct a structured alert payload dictionary
			alert_payload = {
				"timestamp": str(row["timestamp"]),
				"user_id": str(row["user_id"]),
				"amount": float(row["amount"]),
				"merchant": str(row["merchant"]),
				"location": str(row["location"]),
				"fraud_probability": float(row["fraud_probability"]),
				"is_fraud": int(row["is_fraud"])
			}

			if row["is_fraud"] == 1 or row["fraud_probability"] > 0.5:
				print(
					f"🚨 ALERT: HIGH FRAUD RISK FOR {row['user_id']} | Amount: ${row['amount']} | Risk Prob: {prob_percent}%")
				# Route payload to dedicated alerts queue channel
				alert_producer.send('fraud_alerts', alert_payload)
			else:
				print(f"✅ Clean Transaction: {row['user_id']} | Amount: ${row['amount']} | Risk Prob: {prob_percent}%")

		alert_producer.flush()

	print("👀 Active monitoring started. Analyzing incoming Kafka events live...")
	query = structured_df.writeStream \
		.foreachBatch(evaluate_batch_prediction) \
		.start()

	query.awaitTermination()


if __name__ == "__main__":
	main()