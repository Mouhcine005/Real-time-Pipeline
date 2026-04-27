from kafka import KafkaConsumer
from json import loads
from rich import print

# ----------------------
# Kafka Consumer
# ----------------------
consumer = KafkaConsumer(
    'news_topic',  # Topic where news articles are sent
    bootstrap_servers=['localhost:9092'],  # Kafka server addresses
    auto_offset_reset='earliest',  
    enable_auto_commit=False,  # Auto commit offsets
    group_id='viewer',  # Individual consumer (can set a group ID if needed)
    value_deserializer=lambda x: loads(x.decode('utf-8'))  # Deserialize JSON to Python dict
)

# ----------------------
# Process incoming messages
# ----------------------
for message in consumer:
    article = message.value  # Get the value of the message (news article)
    print("[bold green]Title:[/bold green]", article.get("title"))
    print("[cyan]Source:[/cyan]", article.get("source"))
    print("[magenta]Author:[/magenta]", article.get("author"))
    print("[yellow]Published At:[/yellow]", article.get("publishedAt"))
    print("[white]Description:[/white]", article.get("description"))
    print("[blue]URL:[/blue]", article.get("url"))
    print("-" * 80)
