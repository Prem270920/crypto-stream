import json
import os
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_TOPIC = 'bitcoin_real_time'
KAFKA_SERVER = 'localhost:9092'
BATCH_SIZE = 20  
DATA_DIR = "data/processed/stream_history"

os.makedirs(DATA_DIR, exist_ok=True)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to {KAFKA_TOPIC}...")

message_buffer =[]

try:
    for message in consumer:
        # Extract Data
        data = message.value
        message_buffer.append(data)

        print(f"Received: {data['price']} (Buffer: {len(message_buffer)}/{BATCH_SIZE})")

        # "Micro-Batch" Processing
        # once we have 20 messages, save it to a file.
        if len(message_buffer) >= BATCH_SIZE:
            df = pd.DataFrame(message_buffer)

            # Generate a unique filename based on timestamp
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{DATA_DIR}/batch_{timestamp_str}.parquet"

            df.to_parquet(filename, index = False)

            print(f"Saved batch to {filename}")

            message_buffer = []

except KeyboardInterrupt:
    print("\n🛑 Stopping Consumer...")