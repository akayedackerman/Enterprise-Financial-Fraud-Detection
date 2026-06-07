import os
import pickle
from pyspark.sql import SparkSession
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, average_precision_score


def main():
	print("🤖 Initializing Distributed AI Model Training Pipeline...")

	# Authenticate as HDFS superuser
	os.environ["HADOOP_USER_NAME"] = "root"

	# Initialize a Spark Session to load our training records from HDFS
	spark = SparkSession.builder \
		.appName("FraudDetection_ModelTraining") \
		.master("local[*]") \
		.config("spark.driver.bindAddress", "127.0.0.1") \
		.config("spark.driver.host", "127.0.0.1") \
		.config("spark.bindAddress", "127.0.0.1") \
		.config("spark.driver.memory", "4g") \
		.getOrCreate()

	spark.sparkContext.setLogLevel("WARN")

	# 1. Load Engineered Features from HDFS Data Lake
	hdfs_features_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/engineered_features"
	print(f"📖 Ingesting calculated feature matrices from HDFS: {hdfs_features_path}")
	dataset_df = spark.read.parquet(hdfs_features_path)

	# Convert distributed Spark DataFrame to Pandas for local XGBoost training execution
	raw_data = dataset_df.select("amount", "user_tx_count_1h", "user_avg_amount_30d", "is_amount_anomaly").toPandas()
	spark.stop()  # Close spark cluster context as data is loaded into memory

	if raw_data.empty:
		print("❌ Error: The dataset is empty. Ensure your feature pipeline has fully written records to HDFS.")
		return

	# 2. Separate Core Input Features and Target Labels
	X = raw_data[["amount", "user_tx_count_1h", "user_avg_amount_30d"]]
	y = raw_data["is_amount_anomaly"]

	print(f"📊 Dataset successfully structured. Total rows: {len(raw_data)}")
	print(f"📉 Class Balance Profile - Legitimate: {len(y[y == 0])} | Potential Fraud: {len(y[y == 1])}")

	# 3. Train/Test Data Split
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

	# 4. Handle Class Imbalance with Scale_Pos_Weight Calculation
	# This prevents the algorithm from ignoring fraud by heavily penalizing errors on the minority class
	neg_count = len(y_train[y_train == 0])
	pos_count = len(y_train[y_train == 1])
	scale_weight = (neg_count / pos_count) if pos_count > 0 else 1.0

	print(f"⚖️ Setting XGBoost class optimization scaling factor to: {round(scale_weight, 2)}")

	# 5. Initialize and Train the XGBoost Classifier Engine
	print("🚀 Commencing model hyperparameter training...")
	model = XGBClassifier(
		n_estimators=100,
		max_depth=5,
		learning_rate=0.1,
		scale_pos_weight=scale_weight,
		eval_metric="logloss",
		random_state=42
	)
	model.fit(X_train, y_train)

	# 6. Evaluate Model Inference Accuracy
	print("\n📋 Computing Performance Evaluation Matrices:")
	predictions = model.predict(X_test)
	probs = model.predict_proba(X_test)[:, 1]

	print("\n--- Classification Performance Grid ---")
	print(classification_report(y_test, predictions))

	pr_auc = average_precision_score(y_test, probs)
	print(f"🎯 Precision-Recall Area Under Curve (PR-AUC): {round(pr_auc, 4)}")

	# 7. Serialize and Export Model Binary for Live Streaming Inference
	model_export_path = "models/fraud_xgboost_model.pkl"
	print(f"\n💾 Archiving trained classifier binary locally to: {model_export_path}")
	with open(model_export_path, "wb") as model_file:
		pickle.dump(model, model_file)

	print("✅ Machine Learning modeling pipeline completed successfully!")


if __name__ == "__main__":
	main()