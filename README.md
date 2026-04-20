# 📰 Real-Time Multi-Category News Pipeline

A fully automated real-time data engineering pipeline that ingests live news data, processes it using stream processing and NLP, and stores it in both hot and cold storage systems.

---

## 🛠️ Tech Stack

| Layer        | Tool                          |
|--------------|-------------------------------|
| Ingestion    | Python (Requests, NewsAPI)    |
| Streaming    | Apache Kafka                  |
| Processing   | Apache Flink (PyFlink)        |
| NLP          | NLTK                          |
| Hot Storage  | Elasticsearch                 |
| Cold Storage | Hadoop HDFS                   |

---

## 🏗️ Architecture

### Data Ingestion
- Python producer fetches real-time news from NewsAPI
- Supports multiple categories: Technology, Business, Science
- Publishes data to Kafka topic: `news_topic`

### Streaming Layer (Kafka)
- Acts as a distributed message broker
- Decouples ingestion from processing
- Ensures scalability and fault tolerance

### Stream Processing (Flink)
- Cleans and normalizes JSON data
- Handles malformed data safely
- Performs NLP preprocessing (tokenization, stop-word removal)

### Storage

**Elasticsearch**
- Stores structured news data
- Enables real-time analytics and visualization

**HDFS**
- Stores raw JSON data
- Supports long-term storage and batch reprocessing

---

## 🔁 Data Flow

```
NewsAPI → Kafka → Flink → Elasticsearch
                       → HDFS
```

---

## 📂 Project Structure

```
.
├── twitter_kafka_producer.py                 # News ingestion → Kafka
├── twitter_kafka_consumer.py                 # Stream visualization (CLI)
├── kafka_flink_elasticsearch_streaming.py    # Kafka → Flink → Elasticsearch
├── pyflink_word_count.py                     # NLP word frequency pipeline
├── hdfs_consumer.py                          # Kafka → HDFS storage
└── README.md
```

---

## ⚙️ Setup

### 1. Start Required Services

Ensure the following are running before proceeding:

- Kafka & Zookeeper → `localhost:9092`
- Elasticsearch → `localhost:9200`
- Hadoop / HDFS

### 2. Clone Repository

```bash
git clone https://github.com/Mouhcine005/Real-time-Pipeline.git
cd Real-time-Pipeline
```

### 3. Environment Setup

```bash
python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows
# venv\Scripts\activate

pip install kafka-python requests rich pydoop nltk pyflink
```

### 4. Configuration

Create a `.env` file in the project root:

```env
NEWS_API_KEY=your_api_key_here
```

---

## ▶️ Run Pipeline

Run components in the following order:

```bash
# 1. Ingestion
python twitter_kafka_producer.py

# 2. Cold Storage
python hdfs_consumer.py

# 3. Elasticsearch Processing
python kafka_flink_elasticsearch_streaming.py

# 4. NLP Pipeline
python pyflink_word_count.py
```

---

## 🧪 Run Locally (Testing)

```bash
python twitter_kafka_consumer.py
```

---

## 📌 Notes

- Fault-tolerant JSON parsing prevents pipeline crashes
- NLP pipeline uses stop-word removal and normalization
- Elasticsearch enables real-time querying and dashboards
- HDFS ensures long-term data persistence

---

## 👨‍💻 Author

**Mouhcine** 
