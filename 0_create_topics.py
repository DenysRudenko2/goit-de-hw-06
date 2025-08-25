#!/usr/bin/env python3
"""
Створення Kafka топіків для hw-06
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from configs import kafka_config
import json

def create_topics():
    """Створення необхідних топіків"""
    
    # Створення адмін клієнта
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    
    my_name = "denys_rudenko"
    
    # Топіки для створення
    topics_to_create = [
        NewTopic(name=f"{my_name}_sensor_data", num_partitions=3, replication_factor=1),
        NewTopic(name=f"{my_name}_alerts", num_partitions=2, replication_factor=1)
    ]
    
    print("Creating Kafka topics for hw-06:")
    print("-" * 60)
    
    for topic in topics_to_create:
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"✅ Topic '{topic.name}' created successfully")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️  Topic '{topic.name}' already exists")
            else:
                print(f"❌ Error creating topic '{topic.name}': {e}")
    
    print("-" * 60)
    print("\nChecking all topics with your name:")
    all_topics = admin_client.list_topics()
    user_topics = [t for t in all_topics if my_name in t]
    for topic in user_topics:
        print(f"  - {topic}")
    
    admin_client.close()
    
    # Тестове повідомлення для перевірки
    print("\nSending test message to sensor_data topic...")
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    test_message = {
        "id": "sensor_test",
        "temperature": 25.0,
        "humidity": 50.0,
        "timestamp": "2024-11-24T12:00:00"
    }
    
    producer.send(f"{my_name}_sensor_data", value=test_message)
    producer.flush()
    producer.close()
    print("✅ Test message sent successfully")
    
if __name__ == "__main__":
    create_topics()