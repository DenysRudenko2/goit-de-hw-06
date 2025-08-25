#!/usr/bin/env python3
"""
Spark Streaming процесор для агрегації даних датчиків та генерації алертів
Використовує sliding window для обчислення середніх значень та cross join для визначення алертів
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, current_timestamp,
    to_json, struct, lit, when, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType, IntegerType
)
import os

def create_spark_session():
    """Створення Spark сесії з необхідними конфігураціями"""
    
    # Отримуємо поточну директорію
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Створюємо директорію для checkpoints якщо її немає
    checkpoint_dir = os.path.join(current_dir, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Налаштування для Kafka
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
    
    spark = SparkSession.builder \
        .appName("SensorDataStreamingProcessor") \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark, checkpoint_dir

def get_sensor_data_schema():
    """Схема для даних датчиків"""
    return StructType([
        StructField("id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

def read_kafka_stream(spark, kafka_config, topic):
    """Читання потоку даних з Kafka"""
    
    # Формуємо рядок підключення
    kafka_options = {
        "kafka.bootstrap.servers": ",".join(kafka_config['bootstrap_servers']),
        "subscribe": topic,
        "startingOffsets": "earliest",  # Змінено для читання всіх даних
        "kafka.security.protocol": kafka_config['security_protocol'],
        "kafka.sasl.mechanism": kafka_config['sasl_mechanism'],
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    }
    
    # Читаємо потік з Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
    return df

def parse_sensor_data(kafka_df, schema):
    """Парсинг JSON даних з Kafka"""
    
    # Парсимо JSON з value колонки
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.id").alias("sensor_id"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.timestamp").alias("event_time_str"),
        col("kafka_timestamp")
    ).selectExpr(
        "sensor_id",
        "temperature",
        "humidity",
        "to_timestamp(event_time_str) as event_time",
        "kafka_timestamp"
    )
    
    return parsed_df

def aggregate_with_window(sensor_df):
    """
    Агрегація даних з використанням sliding window
    - Window duration: 1 хвилина
    - Sliding interval: 30 секунд  
    - Watermark: 10 секунд
    """
    
    # Додаємо watermark для обробки пізніх даних
    aggregated_df = sensor_df \
        .withWatermark("event_time", "5 seconds") \
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds")  # Зменшено для демонстрації
        ) \
        .agg(
            avg("temperature").alias("t_avg"),
            avg("humidity").alias("h_avg")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("t_avg"),
            col("h_avg")
        )
    
    return aggregated_df

def read_alert_conditions(spark, csv_path):
    """Читання умов алертів з CSV файлу"""
    
    # Читаємо CSV файл
    conditions_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)
    
    # Замінюємо -999 на NULL для правильної фільтрації
    for col_name in ["humidity_min", "humidity_max", "temperature_min", "temperature_max"]:
        conditions_df = conditions_df.withColumn(
            col_name,
            when(col(col_name) == -999, None).otherwise(col(col_name))
        )
    
    return conditions_df

def generate_alerts(aggregated_df, conditions_df):
    """
    Генерація алертів через cross join та фільтрацію
    """
    
    # Cross join для перевірки всіх умов
    alerts_df = aggregated_df.crossJoin(conditions_df)
    
    # Фільтруємо записи, що відповідають умовам алертів
    # Температурні умови
    temp_condition = (
        (col("temperature_min").isNull() | (col("t_avg") >= col("temperature_min"))) &
        (col("temperature_max").isNull() | (col("t_avg") <= col("temperature_max")))
    )
    
    # Умови вологості
    humidity_condition = (
        (col("humidity_min").isNull() | (col("h_avg") >= col("humidity_min"))) &
        (col("humidity_max").isNull() | (col("h_avg") <= col("humidity_max")))
    )
    
    # Фільтруємо алерти, що відповідають умовам
    filtered_alerts = alerts_df.filter(
        temp_condition & humidity_condition
    ).select(
        struct(
            col("window_start").cast("string").alias("start"),
            col("window_end").cast("string").alias("end")
        ).alias("window"),
        col("t_avg"),
        col("h_avg"),
        col("code").cast("string"),
        col("message"),
        current_timestamp().cast("string").alias("timestamp")
    )
    
    return filtered_alerts

def write_alerts_to_kafka(alerts_df, kafka_config, output_topic):
    """Запис алертів у вихідний Kafka топік"""
    
    # Формуємо JSON для відправки
    kafka_output_df = alerts_df.select(
        to_json(struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            col("timestamp")
        )).alias("value")
    )
    
    # Налаштування для запису в Kafka
    kafka_write_options = {
        "kafka.bootstrap.servers": ",".join(kafka_config['bootstrap_servers']),
        "topic": output_topic,
        "kafka.security.protocol": kafka_config['security_protocol'],
        "kafka.sasl.mechanism": kafka_config['sasl_mechanism'],
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    }
    
    # Запис в Kafka
    query = kafka_output_df \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .options(**kafka_write_options) \
        .option("checkpointLocation", f"checkpoints/{output_topic}") \
        .start()
    
    return query

def write_alerts_to_console(alerts_df):
    """Вивід алертів в консоль для моніторингу"""
    
    query = alerts_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query

def main():
    # Імпортуємо конфігурацію Kafka
    from configs import kafka_config
    
    # Налаштування
    my_name = "denys_rudenko"
    input_topic = f"{my_name}_sensor_data"
    output_topic = f"{my_name}_alerts"
    alerts_csv_path = "alerts_conditions_demo.csv"  # Використовуємо демо умови
    
    print("=" * 80)
    print("Starting Spark Streaming Processor")
    print(f"Input topic: {input_topic}")
    print(f"Output topic: {output_topic}")
    print(f"Alert conditions: {alerts_csv_path}")
    print("=" * 80)
    
    # Створюємо Spark сесію
    spark, checkpoint_dir = create_spark_session()
    
    try:
        # 1. Читаємо потік даних з Kafka
        print("Reading sensor data stream from Kafka...")
        kafka_stream = read_kafka_stream(spark, kafka_config, input_topic)
        
        # 2. Парсимо дані датчиків
        sensor_schema = get_sensor_data_schema()
        sensor_stream = parse_sensor_data(kafka_stream, sensor_schema)
        
        # Закоментовуємо діагностику raw даних, щоб не заважала
        # print("Starting raw data monitoring...")
        # raw_query = sensor_stream \
        #     .writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .option("truncate", False) \
        #     .trigger(processingTime='5 seconds') \
        #     .start()
        
        # 3. Агрегуємо з sliding window
        print("Aggregating data with sliding window (1 min window, 30 sec slide)...")
        aggregated_stream = aggregate_with_window(sensor_stream)
        
        # 4. Читаємо умови алертів
        print(f"Reading alert conditions from {alerts_csv_path}...")
        conditions_df = read_alert_conditions(spark, alerts_csv_path)
        conditions_df.show()
        
        # 5. Генеруємо алерти через cross join
        print("Generating alerts based on conditions...")
        alerts_stream = generate_alerts(aggregated_stream, conditions_df)
        
        # 6. Записуємо алерти в Kafka та консоль
        print(f"Writing alerts to Kafka topic: {output_topic}")
        kafka_query = write_alerts_to_kafka(alerts_stream, kafka_config, output_topic)
        
        print("Writing alerts to console for monitoring...")
        console_query = write_alerts_to_console(alerts_stream)
        
        # Додатковий вивід агрегованих даних для діагностики
        print("Monitoring aggregated windows...")
        agg_console_query = aggregated_stream \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Чекаємо завершення
        print("\nStreaming started. Press Ctrl+C to stop...")
        print("-" * 80)
        
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\nStopping streaming...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("Spark session closed.")

if __name__ == "__main__":
    main()