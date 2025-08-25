#!/usr/bin/env python3
"""
Генератор даних від IoT датчиків для Kafka
Створює потік даних з id, temperature, humidity, timestamp
"""

from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random
from datetime import datetime
import sys

def create_producer():
    """Створення Kafka Producer"""
    return KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: str(v).encode('utf-8')
    )

def generate_sensor_data(sensor_id):
    """Генерація даних датчика"""
    # Генеруємо температуру та вологість з різними діапазонами для демонстрації алертів
    temp_ranges = [
        (20, 35),   # Нормальна температура
        (35, 40),   # Висока температура
        (40, 50),   # Критично висока температура
    ]
    
    humidity_ranges = [
        (10, 20),   # Низька вологість
        (20, 60),   # Нормальна вологість
        (60, 80),   # Висока вологість
        (80, 95),   # Критично висока вологість
    ]
    
    # Вибираємо випадковий діапазон
    temp_range = random.choice(temp_ranges)
    humidity_range = random.choice(humidity_ranges)
    
    return {
        "id": sensor_id,
        "temperature": round(random.uniform(*temp_range), 2),
        "humidity": round(random.uniform(*humidity_range), 2),
        "timestamp": datetime.now().isoformat()
    }

def main():
    # Ім'я користувача для унікальності топіків
    my_name = "denys_rudenko"
    topic_name = f"{my_name}_sensor_data"
    
    # Генеруємо унікальний префікс для цього екземпляру
    import os
    instance_id = os.getpid() % 1000  # Використовуємо PID для унікальності
    
    # Кількість датчиків для цього екземпляру
    num_sensors = 5
    sensor_ids = [f"sensor_{instance_id:03d}_{i:02d}" for i in range(1, num_sensors + 1)]
    
    print(f"Starting sensor data generator for topic: {topic_name}")
    print(f"Instance ID: {instance_id} (PID: {os.getpid()})")
    print(f"Generating data for {num_sensors} sensors: {sensor_ids}")
    print("-" * 60)
    
    producer = create_producer()
    
    try:
        message_count = 0
        while True:
            # Генеруємо дані для випадкового датчика
            sensor_id = random.choice(sensor_ids)
            sensor_data = generate_sensor_data(sensor_id)
            
            # Відправляємо в Kafka
            producer.send(topic_name, key=sensor_id, value=sensor_data)
            
            message_count += 1
            print(f"[{message_count:5d}] {sensor_id}: "
                  f"T={sensor_data['temperature']:5.1f}°C, "
                  f"H={sensor_data['humidity']:5.1f}%, "
                  f"Time={sensor_data['timestamp']}")
            
            # Інтервал між повідомленнями (0.5-2 секунди)
            time.sleep(random.uniform(0.5, 2))
            
            # Flush кожні 10 повідомлень
            if message_count % 10 == 0:
                producer.flush()
                
    except KeyboardInterrupt:
        print(f"\nStopping... Total messages sent: {message_count}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()