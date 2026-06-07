import streamlit as st
import json
import pandas as pd
from kafka import KafkaConsumer
import time

st.set_page_config(
	page_title="Enterprise Fraud Security Console",
	page_icon="🛡️",
	layout="wide"
)

st.title("🛡️ Enterprise Financial Fraud Real-Time Security Console")
st.markdown("Live monitoring of suspicious transactions flagged by the streaming XGBoost pipeline.")

# 1. Layout Metrics Containers are created FIRST so the page never loads blank
col1, col2, col3 = st.columns(3)
total_alerts_metric = col1.metric("Total Fraud Alerts Blocked", "0")
total_risk_value = col2.metric("Total Intercepted Risk Capital", "$0.00")
highest_risk_factor = col3.metric("Highest Threat Probability", "0.0%")

st.subheader("🔥 Real-Time Flagged Alerts Ingestion Feed")
table_placeholder = st.empty()

# Persistent session state tracking arrays
if "alerts_history" not in st.session_state:
	st.session_state.alerts_history = []


# Connect cleanly to our streaming alerts channel broker
@st.cache_resource
def get_kafka_consumer():
    try:
        return KafkaConsumer(
            'fraud_alerts',
            bootstrap_servers=['127.0.0.1:9092'],
            client_id='streamlit_dashboard_monitor',
            group_id='security_console_viewers',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            # Swapped parameter name to use the correct deserialization decoder
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=200,
            api_version=(2, 5, 0)
        )
    except Exception as e:
        return str(e)

consumer_res = get_kafka_consumer()

# Check if the result is our consumer object or an error string
if isinstance(consumer_res, str):
    st.error(f"❌ True Connection Error: {consumer_res}")
    consumer = None
else:
    consumer = consumer_res

# 2. Render whatever historical logs we have collected so far
if st.session_state.alerts_history:
	df = pd.DataFrame(st.session_state.alerts_history)

	total_count = len(df)
	sum_value = df["amount"].sum()
	max_prob = df["fraud_probability"].max() * 100

	total_alerts_metric.metric("Total Fraud Alerts Blocked", str(total_count))
	total_risk_value.metric("Total Intercepted Risk Capital", f"${sum_value:,.2f}")
	highest_risk_factor.metric("Highest Threat Probability", f"{max_prob:.2f}%")

	display_df = df[["timestamp", "user_id", "amount", "merchant", "location", "fraud_probability"]]
	display_df["fraud_probability"] = display_df["fraud_probability"].apply(lambda x: f"{round(x * 100, 2)}%")
	table_placeholder.dataframe(display_df, use_container_width=True)
else:
	table_placeholder.info("🟢 Listening for live streaming security alerts... No fraud flags intercepted yet.")

# 3. Pull new alerts from Kafka, save them, and refresh the UI page quickly
if consumer:
	new_alerts = False
	# Fetch messages out of consumer buffer partition blocks
	for message in consumer:
		st.session_state.alerts_history.insert(0, message.value)
		new_alerts = True

	# Trim container bounds to prevent browser performance bottlenecks
	if len(st.session_state.alerts_history) > 50:
		st.session_state.alerts_history = st.session_state.alerts_history[:50]

	# Only cycle browser execution redraw frames if new data was actually collected
	time.sleep(0.4)
	st.rerun()
else:
	st.warning("⚠️ Retrying broker connectivity... Ensure your Kafka infrastructure is fully online.")
	time.sleep(2.0)
	st.rerun()