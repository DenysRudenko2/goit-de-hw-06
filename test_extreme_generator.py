#!/usr/bin/env python3
"""
Генератор екстремальних даних для тестування алертів
"""

from kafka import KafkaProducer
from configs import kafka_config
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: str(v).encode('utf-8')
)

my_name = "denys_rudenko"
topic_name = f"{my_name}_sensor_data"

print(f"Generating EXTREME values for alerts testing")
print("-" * 60)

scenarios = [
    {"temp": 45.0, "hum": 10.0, "expected": "Too hot + Too dry"},  # Code 104 + 101
    {"temp": 25.0, "hum": 85.0, "expected": "Too cold + Too wet"},  # Code 103 + 102
    {"temp": 50.0, "hum": 95.0, "expected": "Too hot + Too wet"},   # Code 104 + 102
    {"temp": 15.0, "hum": 5.0, "expected": "Too cold + Too dry"},   # Code 103 + 101
]

try:
    for i in range(100):
        scenario = scenarios[i % len(scenarios)]
        
        data = {
            "id": f"test_sensor_{i % 3}",
            "temperature": scenario["temp"],
            "humidity": scenario["hum"],
            "timestamp": datetime.now().isoformat()
        }
        
        producer.send(topic_name, key=data["id"], value=data)
        
        print(f"[{i:3d}] T={data['temperature']:5.1f}°C, H={data['humidity']:5.1f}% - {scenario['expected']}")
        
        time.sleep(0.5)  # Швидка генерація
        
        if i % 10 == 0:
            producer.flush()
            
except KeyboardInterrupt:
    print("\nStopped")
finally:
    producer.flush()
    producer.close()