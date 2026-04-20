from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udf, udtf, ScalarFunction
import string
import re
import nltk

# Ensure stopwords are downloaded locally
nltk.download('stopwords')

# ----------------------
# NLP Functions
# ----------------------

@udf(result_type=DataTypes.STRING())
def preprocess_text(text):
    if text is None: 
        return ""
    # Remove URLs, mentions, and non-alphabetic characters
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', ' ', text)
    text = re.sub('@[^\s]+', ' ', text)
    text = re.sub('[^a-zA-Z]+', ' ', text)
    return text.lower().strip()

@udtf(result_types=[DataTypes.STRING()])
def split_udtf(line):
    """
    Fixed: Explicitly handles Row objects by converting to string.
    """
    if line is not None:
        # Convert to string to handle Row(f0='text') issues
        content = str(line)
        # If Flink returns it as "Row(f0='text')", we strip the wrapper
        if content.startswith("Row("):
            content = content.split("'", 1)[1].rsplit("'", 1)[0]
            
        for s in content.split():
            if len(s) > 2: 
                yield s

@udf(result_type=DataTypes.STRING())
def normalize(word):
    if word is None:
        return ""
    return word.translate(str.maketrans('', '', string.punctuation)).strip()

class RemoveStopWord(ScalarFunction):
    def __init__(self):
        from nltk.corpus import stopwords
        self.stops = set(stopwords.words('english'))
        
    def eval(self, word: str):
        if word is None or word == "":
            return False
        return word not in self.stops

# ----------------------
# Main Pipeline
# ----------------------

def word_count_stream_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # --- Configuration ---
    # Update this path if your venv is located elsewhere
    python_path = "/home/refoli/kafka_twitter_project/venv/bin/python"
    config = t_env.get_config().get_configuration()
    
    t_env.get_config().set_python_executable(python_path)
    config.set_string("python.client.executable", python_path)
    
    # Load Jars
    config.set_string("pipeline.jars", 
        "file:///home/refoli/flink/lib/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar;"
        "file:///home/refoli/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    # 2. Define Source Table (Kafka)
    # Ensure scan.startup.mode is latest-offset so it doesn't crash on old data
    source_ddl = """
        CREATE TABLE source_table(
            title STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'news_topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink_news_wordcount_v3',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """

    # 3. Define Sink Table (Elasticsearch)
    sink_ddl = """
        CREATE TABLE sink_table(
            word STRING,
            number BIGINT,
            PRIMARY KEY (word) NOT ENFORCED     
        ) WITH (
            'connector' = 'elasticsearch-7',
            'index' = 'news_wordcount',
            'hosts' = 'http://localhost:9200',
            'format' = 'json',
            'sink.bulk-flush.max-actions' = '1'
        )
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # Register the StopWord Class UDF
    remove_stop_words_udf = udf(RemoveStopWord(), result_type=DataTypes.BOOLEAN())

    # 4. Pipeline Logic
    source_table = t_env.from_path('source_table')
    
    result = source_table \
        .select(preprocess_text(col('title')).alias('line')) \
        .flat_map(split_udtf(col('line'))).alias('word') \
        .select(normalize(col('word')).alias('word')) \
        .filter(col('word') != '') \
        .filter(remove_stop_words_udf(col('word'))) \
        .group_by(col('word')) \
        .select(col('word'), col('word').count.alias('number'))

    # 5. Execute
    print("Starting Word Count Stream Processing...")
    try:
        result.execute_insert('sink_table').wait()
    except Exception as e:
        print(f"Pipeline Error: {e}")

if __name__ == '__main__':
    word_count_stream_processing()