import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

PRODUCER = KafkaProducer(
	bootstrap_servers=['localhost:9092'],
	value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'financial_transactions'


def generate_transaction():
	user_ids = [f"USR_{i:04d}" for i in range(1, 100)]
	merchants = ["Amazon", "Target", "Steam_Games", "BestBuy", "GasStation_76", "Unknown_Vendor"]
	user = random.choice(user_ids)
	amount = round(random.uniform(5.0, 1200.0), 2)

	if random.random() < 0.05:
		amount = round(random.uniform(5000.0, 10000.0), 2)

	return {
		"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
		"user_id": user,
		"amount": amount,
		"merchant": random.choice(merchants),
		"location": random.choice(["New York", "San Francisco", "Shanghai", "London", "Online"])
	}


if __name__ == "__main__":
	print("🚀 Kafka Transaction Simulator Online. Press Ctrl+C to terminate.")
	try:
		while True:
			tx_data = generate_transaction()
			PRODUCER.send(TOPIC_NAME, value=tx_data)
			print(f"Sent: {tx_data}")
			time.sleep(random.uniform(0.2, 1.5))
	except KeyboardInterrupt:
		print("\nStopping simulator...")
	finally:
		PRODUCER.close()