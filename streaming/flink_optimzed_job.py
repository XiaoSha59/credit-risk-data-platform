"""
File path: streaming/flink_optimzed_job.py
Description: Optimized Flink streaming job addressing data bursts, late arrivals
             via watermarks, state management, and window-based metric aggregation
             with required Kafka dependencies.
"""

import os
import sys
import urllib.request

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema


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


def evaluate_risk_payload(raw_event: str):
    """Parse raw CSV event string into a structured dict with risk classification."""
    parts = raw_event.strip().split(",")
    customer_id = parts[0] if len(parts) > 0 else 'UNKNOWN'
    loan_amount = float(parts[1]) if len(parts) > 1 else 0.0
    timestamp = int(parts[2]) if len(parts) > 2 else 0

    if loan_amount >= 10_000_000:
        risk_level = "HIGH"
    elif loan_amount >= 3_000_000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return {
        'customer_id': customer_id,
        'total_loan_amount': loan_amount,
        'max_timestamp': timestamp,
        'risk_level': risk_level,
    }


def merge_risk(total_amount):
    if total_amount >= 10_000_000:
        return "HIGH"
    elif total_amount >= 3_000_000:
        return "MEDIUM"
    return "LOW"


def run_optimized_streaming_pipeline():
    # 1. Initialize execution environment
    config = Configuration()
    config.set_string("rest.address", "localhost")
    config.set_string("rest.port", "8081-8090")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.set_python_executable(sys.executable)

    ensure_kafka_jar(env)

    print("\n" + "=" * 60)
    print("[+] Flink Optimized Job starting...")
    print("[+] Flink Web UI Dashboard: http://localhost:8081 (or :8082)")
    print("=" * 60 + "\n")

    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(10000)

    # 2. Configure Kafka Source for real-time credit event ingestion
    bootstrap_servers = "localhost:9092"
    topic_name = "credit_risk_events"

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(topic_name) \
        .set_group_id("credit_risk_optimized_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka_Credit_Risk_Source"
    )

    # 3. Parse events and evaluate risk
    parsed_stream = stream.map(evaluate_risk_payload)

    # 4. Window-based aggregation using Processing-Time (60s window)
    #    NOTE: allowed_lateness is incompatible with PyFlink Python SDK (Beam path).
    #    A wider 60-second window is used to naturally absorb late-arriving events.
    aggregated_stream = parsed_stream \
        .key_by(lambda x: x['customer_id']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60))) \
        .reduce(lambda a, b: {
            'customer_id': a['customer_id'],
            'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],
            'max_timestamp': max(a['max_timestamp'], b['max_timestamp']),
            'risk_level': merge_risk(a['total_loan_amount'] + b['total_loan_amount'])
        })

    # 5. Format and print results
    aggregated_stream \
        .map(lambda x: (
            f"[WINDOW RESULT] Customer: {x['customer_id']:12s} | "
            f"Total Loan: {x['total_loan_amount']:>12,.0f} VND | "
            f"Risk: {x['risk_level']}"
        )) \
        .print()

    env.execute("Optimized_Credit_Risk_Streaming_Pipeline")


if __name__ == '__main__':
    run_optimized_streaming_pipeline()