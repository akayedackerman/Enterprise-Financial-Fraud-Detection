import os
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, precision_recall_curve, auc
from pyspark.sql import SparkSession
import pickle
import mlflow
import mlflow.xgboost


def main():
	print("🤖 Initializing Distributed AI Model Training Pipeline with MLflow...")

	# 1. Initialize local Spark session to pull features from HDFS
	spark = SparkSession.builder \
		.appName("FraudDetection_ModelTraining") \
		.master("local[*]") \
		.config("spark.driver.bindAddress", "127.0.0.1") \
		.config("spark.driver.host", "127.0.0.1") \
		.getOrCreate()

	spark.sparkContext.setLogLevel("WARN")

	hdfs_input_path = "hdfs://localhost:8020/user/hadoop/fraud_detection/engineered_features"
	print(f"📖 Ingesting calculated feature matrices from HDFS: {hdfs_input_path}")

	dataset_df = spark.read.parquet(hdfs_input_path)
	raw_data = dataset_df.select("amount", "user_tx_count_1h", "user_avg_amount_30d", "is_fraudulent_claim").toPandas()
	spark.stop()

	if raw_data.empty:
		print("❌ Error: The dataset ingested from HDFS is empty.")
		return

	print(f"📊 Dataset successfully structured. Total rows: {len(raw_data)}")

	# Calculate Class Balances
	class_counts = raw_data["is_fraudulent_claim"].value_counts()
	legit_count = class_counts.get(0, 0)
	fraud_count = class_counts.get(1, 0)
	print(f"📉 Class Balance Profile - Legitimate: {legit_count} | Potential Fraud: {fraud_count}")

	# Calculate optimal XGBoost scale_pos_weight
	scale_pos_weight_value = round(legit_count / fraud_count, 2) if fraud_count > 0 else 1.0
	print(f"⚖️ Setting XGBoost class optimization scaling factor to: {scale_pos_weight_value}")

	X = raw_data[["amount", "user_tx_count_1h", "user_avg_amount_30d"]]
	y = raw_data["is_fraudulent_claim"]

	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

	# 2. Configure MLflow Experiment Context Tracking Workspace
	mlflow.set_experiment("Enterprise_Financial_Fraud_Detection")

	with mlflow.start_run(run_name="XGBoost_Behavioral_Training"):
		# Configure model parameters
		xgb_params = {
			"n_estimators": 100,
			"max_depth": 5,
			"learning_rate": 0.1,
			"scale_pos_weight": scale_pos_weight_value,
			"random_state": 42,
			"eval_metric": "logloss"
		}

		# Log training configuration hyper-parameters straight to MLflow
		mlflow.log_params(xgb_params)

		print("🚀 Commencing model hyperparameter training...")
		model = XGBClassifier(**xgb_params)
		model.fit(X_train, y_train)

		# Evaluate Performance Matrices
		y_pred = model.predict(X_test)
		y_probs = model.predict_proba(X_test)[:, 1]

		report = classification_report(y_test, y_pred, output_dict=True)

		precision, recall, _ = precision_recall_curve(y_test, y_probs)
		pr_auc_score = round(auc(recall, precision), 4)

		print("\n📋 Computing Performance Evaluation Matrices:")
		print("\n--- Classification Performance Grid ---")
		print(classification_report(y_test, y_pred))
		print(f"🎯 Precision-Recall Area Under Curve (PR-AUC): {pr_auc_score}\n")

		# Log metrics to MLflow dashboard
		mlflow.log_metric("pr_auc", pr_auc_score)
		mlflow.log_metric("legit_f1", round(report["0"]["f1-score"], 4))
		mlflow.log_metric("fraud_f1", round(report["1"]["f1-score"], 4))
		mlflow.log_metric("accuracy", round(report["accuracy"], 4))

		# 3. Model Registry Serialization Logging
		# Log model directly to MLflow workspace registry database storage
		mlflow.xgboost.log_model(model, artifact_path="fraud_xgboost_model")

		# Keep saving a local fallback copy for our streaming scripts
		os.makedirs("models", exist_ok=True)
		local_pkl_path = "models/fraud_xgboost_model.pkl"
		with open(local_pkl_path, "wb") as f:
			pickle.dump(model, f)
		print(f"💾 Archiving trained classifier binary locally to: {local_pkl_path}")
		print("✅ Machine Learning modeling pipeline tracked and completed successfully!")


if __name__ == "__main__":
	main()