"""
File path: streaming/kafka_flink_source.py
Description: Integrates Apache Kafka as the real-time event streaming source 
             for the Flink credit risk processing engine. Handles live loan 
             application events and transaction streams.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

def create_kafka_streaming_source():
    # 1. Initialize Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # 2. Configure Kafka Source parameters
    # Consuming real-time loan application events from Kafka topic 'credit_risk_events'
    bootstrap_servers = "localhost:9092"
    topic_name = "credit_risk_events"
    consumer_group_id = "credit_risk_flink_group"

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(topic_name) \
        .set_group_id(consumer_group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Ingest data stream from Kafka into Flink
    stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.no_watermarks(), # Will be enhanced with bounded out-of-orderness next
        "Kafka_Credit_Risk_Source"
    )

    # Print raw incoming streaming records for monitoring
    stream.print()

    # Execute the streaming pipeline
    env.execute("Kafka_Flink_Realtime_Ingestion")

if __name__ == '__main__':
    create_kafka_streaming_source()