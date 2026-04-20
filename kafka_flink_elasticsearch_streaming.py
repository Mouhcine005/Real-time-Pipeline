from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///home/refoli/flink/lib/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar;"
    "file:///home/refoli/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
)

# --- SOURCE: Use 'raw' format to avoid Deserialization errors ---
source_ddl = """
CREATE TABLE source_table(
    raw_line STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'news_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink_final_fix',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
)
"""
t_env.execute_sql(source_ddl)

# --- SINK: Standard ES Sink ---
sink_ddl = """
CREATE TABLE sink_table(
    source_name STRING,
    author STRING,
    title STRING,
    description STRING,
    url STRING,
    publishedAt STRING,
    content STRING
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://localhost:9200',
    'index' = 'news_kafka_flink_safe',
    'format' = 'json',
    'sink.bulk-flush.max-actions' = '1'
)
"""
t_env.execute_sql(sink_ddl)

# --- TRANSFORMATION: Use JSON_VALUE (This is the secret sauce) ---
# JSON_VALUE won't crash if a field is missing or weirdly formatted.
processed_table = t_env.sql_query("""
    SELECT 
        COALESCE(JSON_VALUE(raw_line, '$.source'), 'Unknown') as source_name,
        COALESCE(JSON_VALUE(raw_line, '$.author'), 'Unknown') as author,
        COALESCE(JSON_VALUE(raw_line, '$.title'), 'No Title') as title,
        COALESCE(JSON_VALUE(raw_line, '$.description'), '') as description,
        COALESCE(JSON_VALUE(raw_line, '$.url'), 'no-url') as url,
        COALESCE(JSON_VALUE(raw_line, '$.publishedAt'), '') as publishedAt,
        COALESCE(JSON_VALUE(raw_line, '$.content'), '') as content
    FROM source_table
""")

print("Starting Stable Flink Job...")
processed_table.execute_insert('sink_table').wait()