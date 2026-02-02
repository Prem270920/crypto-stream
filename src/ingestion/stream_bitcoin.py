import json
import time
import requests
from kafka import KafkaProducer

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'bitcoin_real_time'
API_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🚀 Starting Producer. Sending to: {TOPIC_NAME}")

try:
    while True:
        try:
            #Fetch Real-Time Data
            response = requests.get(API_URL)
            data = response.json()
            
            # Coinbase JSON: {'data': {'base': 'BTC', 'currency': 'USD', 'amount': '42000.50'}}
            if 'data' in data:
                price = data['data']['amount']
                
                message = {
                    'timestamp': int(time.time() * 1000), # Current time in ms
                    'price': float(price),
                    'source': 'coinbase'
                }
                
                #Send to Kafka
                producer.send(TOPIC_NAME, message)
                print(f"Sent: {message}")
                producer.flush()
            
            time.sleep(5)
            
        except Exception as e:
            print(f"API Error: {e}")
            time.sleep(5)

except KeyboardInterrupt:
    print("\n Stopping Producer...")
    producer.close()
