from pyflink.table import TableEnvironment, EnvironmentSettings

# Create streaming Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Add Kafka connector JAR (CHANGE PATH IF NEEDED)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///home/refoli/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
)

# Kafka Source Table
source_ddl = """
CREATE TABLE twitter_source (
    id_str STRING,
    username STRING,
    tweet STRING,
    location STRING,
    retweet_count BIGINT,
    favorite_count BIGINT,
    followers_count BIGINT,
    lang STRING,
    hashtags ARRAY<STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'twitter_tweets',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink_twitter_group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
"""

# Register source table
t_env.execute_sql(source_ddl)

# Simple streaming query (read everything)
result_table = t_env.sql_query("""
    SELECT
        username,
        tweet,
        lang,
        retweet_count,
        followers_count
    FROM twitter_source
""")

# Print stream to console
result_table.execute().print()
