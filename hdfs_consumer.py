from kafka import KafkaConsumer
from json import dumps, loads
from rich import print
import pydoop.hdfs as hdfs
import datetime

# ----------------------
# Kafka consumer
# ----------------------
consumer = KafkaConsumer(
    'news_topic',               # Your Kafka topic
    bootstrap_servers=['localhost:9092'],  # Kafka broker(s)
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("[INFO] Kafka consumer started...")

# ----------------------
# HDFS path to store news
# ----------------------
hdfs_path = '/data/news/raw/all_articles.json'

# ----------------------
# Process incoming messages
# ----------------------
for message in consumer:
    article = message.value
    print("[INFO] Received article:", article.get("title"))

    try:
        # Append the article as a JSON line
        with hdfs.open(hdfs_path, 'at') as file:
            file.write(f"{dumps(article)}\n")
            print("[INFO] Stored in HDFS!")
    except Exception as e:
        print(f"[ERROR] Failed to write to HDFS: {e}")
