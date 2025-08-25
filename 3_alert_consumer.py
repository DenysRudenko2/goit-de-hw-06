#!/usr/bin/env python3
"""
Споживач алертів з Kafka для моніторингу та відображення
"""

from kafka import KafkaConsumer
from configs import kafka_config
import json
from datetime import datetime

def create_consumer(topic):
    """Створення Kafka Consumer"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert_monitor_group'
    )

def format_alert(alert_data):
    """Форматування алерту для виводу"""
    window = alert_data.get('window', {})
    
    output = []
    output.append("=" * 80)
    output.append(f"🚨 ALERT: {alert_data.get('message', 'Unknown')}")
    output.append("-" * 80)
    output.append(f"📊 Code:        {alert_data.get('code', 'N/A')}")
    output.append(f"🌡️  Temperature: {alert_data.get('t_avg', 0):.2f}°C")
    output.append(f"💧 Humidity:    {alert_data.get('h_avg', 0):.2f}%")
    output.append(f"⏰ Window:      {window.get('start', 'N/A')} to {window.get('end', 'N/A')}")
    output.append(f"🕐 Alert Time:  {alert_data.get('timestamp', 'N/A')}")
    output.append("=" * 80)
    
    return "\n".join(output)

def main():
    # Налаштування
    my_name = "denys_rudenko"
    alerts_topic = f"{my_name}_alerts"
    
    print("=" * 80)
    print("🔔 Alert Monitoring System")
    print(f"📡 Monitoring topic: {alerts_topic}")
    print(f"🏷️  Consumer group: alert_monitor_group")
    print("=" * 80)
    print("Waiting for alerts...\n")
    
    # Створюємо consumer
    consumer = create_consumer(alerts_topic)
    
    # Лічильники за типами алертів
    alert_counts = {
        "101": 0,  # Too dry
        "102": 0,  # Too wet
        "103": 0,  # Too cold
        "104": 0,  # Too hot
    }
    
    try:
        for message in consumer:
            alert_data = message.value
            
            # Оновлюємо лічильники
            code = str(alert_data.get('code', ''))
            if code in alert_counts:
                alert_counts[code] += 1
            
            # Виводимо форматований алерт
            print(format_alert(alert_data))
            
            # Статистика
            print("\n📈 Statistics:")
            print(f"   Too dry:  {alert_counts['101']}")
            print(f"   Too wet:  {alert_counts['102']}")
            print(f"   Too cold: {alert_counts['103']}")
            print(f"   Too hot:  {alert_counts['104']}")
            print(f"   Total:    {sum(alert_counts.values())}")
            print()
            
    except KeyboardInterrupt:
        print("\n" + "=" * 80)
        print("Monitoring stopped.")
        print(f"Final statistics:")
        print(f"   Too dry:  {alert_counts['101']}")
        print(f"   Too wet:  {alert_counts['102']}")
        print(f"   Too cold: {alert_counts['103']}")
        print(f"   Too hot:  {alert_counts['104']}")
        print(f"   Total:    {sum(alert_counts.values())}")
        print("=" * 80)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()