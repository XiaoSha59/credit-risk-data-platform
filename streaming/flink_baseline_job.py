"""
File path: streaming/flink_baseline_job.py
Description: Baseline Flink streaming job connected to Kafka with required 
             Kafka connector JAR dependencies configured for the PyFlink JVM runtime.
"""

import os
import sys
import urllib.request

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


def ensure_kafka_jar(env):
    """
    Ensure the Flink Kafka Connector JAR exists locally in plugins/
    and add it to the PyFlink StreamExecutionEnvironment.
    """
    jar_dir = os.path.abspath("plugins")
    os.makedirs(jar_dir, exist_ok=True)
    jar_path = os.path.join(jar_dir, "flink-sql-connector-kafka-4.0.0-2.0.jar")
    if not os.path.exists(jar_path) or os.path.getsize(jar_path) == 0:
        print(f"[*] Downloading Flink Kafka connector JAR to {jar_path}...")
        url = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar"
        urllib.request.urlretrieve(url, jar_path)
    
    file_url = "file:///" + jar_path.replace("\\", "/")
    env.add_jars(file_url)
    print(f"[*] Loaded Kafka connector JAR: {file_url}")


def run_baseline_streaming():
    # 1. Initialize environment with Web UI configured on port range 8081-8090
    config = Configuration()
    config.set_string("rest.address", "localhost")
    config.set_string("rest.port", "8081-8090")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.set_python_executable(sys.executable)
    
    ensure_kafka_jar(env)

    print("\n" + "=" * 60)
    print("[+] Flink Web UI Dashboard: http://localhost:8081")
    print("=" * 60 + "\n")

    # 2. Configure Kafka Source for real-time events
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("credit_risk_events") \
        .set_group_id("credit_risk_baseline_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka_Baseline_Source"
    )

    # 3. Naive processing using processing-time windows
    result_stream = stream \
        .map(lambda x: evaluate_risk_payload(x)) \
        .key_by(lambda x: x['customer_id']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: {
            'customer_id': a['customer_id'],
            'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],
            'max_timestamp': max(a['max_timestamp'], b['max_timestamp'])
        })

    result_stream.print()

    env.execute("CreditRisk_Baseline_Streaming_Job")

def evaluate_risk_payload(raw_event: str):
    parts = raw_event.split(",")
    return {
        'customer_id': parts[0] if len(parts) > 0 else 'UNKNOWN',
        'total_loan_amount': float(parts[1]) if len(parts) > 1 else 0.0,
        'max_timestamp': int(parts[2]) if len(parts) > 2 else 0
    }

if __name__ == '__main__':
    run_baseline_streaming()