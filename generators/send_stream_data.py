"""
Module: send_stream_data.py
Description: Continuously produces realistic credit risk transaction events to Kafka topic 'credit_risk_events'
             for PyFlink streaming job testing.
"""

import time
import random
from kafka import KafkaProducer


def run_producer(n_events=100, delay=0.5):
    bootstrap_servers = 'localhost:9092'
    topic = 'credit_risk_events'

    print(f"[*] Connecting to Kafka at {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v.encode('utf-8')
    )

    customer_pool = [f"CUST-{i}" for i in range(1001, 1010)]

    print(f"[+] Starting event producer... Sending {n_events} events to topic '{topic}' (delay={delay}s)\n")

    for i in range(1, n_events + 1):
        cust_id = random.choice(customer_pool)
        amount = random.choice([500000, 1000000, 2500000, 5000000, 12000000])
        current_ts = int(time.time())

        # Payload format expected by flink_baseline_job: customer_id,total_loan_amount,max_timestamp
        payload = f"{cust_id},{amount},{current_ts}"
        
        producer.send(topic, value=payload)
        print(f"[{i}/{n_events}] Produced Event -> Topic: '{topic}' | Payload: {payload}")
        time.sleep(delay)

    producer.flush()
    print("\n[+] Done sending streaming events!")


if __name__ == '__main__':
    run_producer(n_events=100, delay=0.5)
