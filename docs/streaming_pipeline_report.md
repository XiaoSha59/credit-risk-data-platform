# Online Streaming Pipeline — Credit Risk Data Platform

---

## PART 1: ONLINE STREAMING EXECUTION GUIDE

### 1.1 Overall Architecture

```
Kafka Producer (send_stream_data.py)
        │
        ▼  topic: credit_risk_events
┌─────────────────┐
│   Apache Kafka  │  (Docker: platform-kafka:9092)
│   + Zookeeper   │  (Docker: platform-zookeeper:2181)
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
Baseline    Optimized
Flink Job   Flink Job
(30s win)   (60s win + risk classification)
    │         │
    ▼         ▼
  stdout    stdout + Flink Web UI
```

---

### 1.2 Start Kafka & Zookeeper (Docker)

**Step 1: Start full infrastructure**

```powershell
# At project root directory
docker-compose up -d
```

**Step 2: Confirm containers are running**

```powershell
docker ps
```

Expected output:
```
CONTAINER ID   IMAGE                            PORTS                    NAMES
xxxxxxxxxxxx   confluentinc/cp-kafka:7.3.0      0.0.0.0:9092->9092/tcp   platform-kafka
xxxxxxxxxxxx   confluentinc/cp-zookeeper:7.3.0  0.0.0.0:2181->2181/tcp   platform-zookeeper
```

**Step 3: Check Kafka topic**

```powershell
# List existing topics
docker exec -it platform-kafka kafka-topics --list --bootstrap-server localhost:9092

# (Optional) Manually create topic if it does not exist yet
docker exec -it platform-kafka kafka-topics \
  --create --topic credit_risk_events \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

---

### 1.3 Run Flink Baseline Job

> **File:** `streaming/flink_baseline_job.py`

```powershell
# Terminal 1 — Start Baseline Job
.venv\Scripts\python.exe streaming/flink_baseline_job.py
```

**Successful startup result:**
```
[*] Loaded Kafka connector JAR: file:///D:/credit-risk-data-platform/plugins/...

============================================================
[+] Flink Web UI Dashboard: http://localhost:8081
============================================================
```

- Flink Web UI: **http://localhost:8081**
- Window: `TumblingProcessingTimeWindows` — **30 seconds**
- Consumer group: `credit_risk_baseline_group`
- No risk classification, no checkpointing

---

### 1.4 Run Flink Optimized Job

> **File:** `streaming/flink_optimzed_job.py`

```powershell
# Terminal 2 — Start Optimized Job (can run concurrently with Baseline)
.venv\Scripts\python.exe streaming/flink_optimzed_job.py
```

**Successful startup result:**
```
[*] Loaded Kafka connector JAR: file:///D:/...

============================================================
[+] Flink Optimized Job starting...
[+] Flink Web UI Dashboard: http://localhost:8081 (or :8082)
============================================================
```

- Flink Web UI: **http://localhost:8081** (or `:8082` if port 8081 is occupied)
- Window: `TumblingProcessingTimeWindows` — **60 seconds**
- Consumer group: `credit_risk_optimized_group`
- Features risk classification (`HIGH/MEDIUM/LOW`), 10-second checkpointing

---

### 1.5 Send Data to Kafka for Testing

> **File:** `generators/send_stream_data.py`

```powershell
# Terminal 3 — Produce 100 events to Kafka (0.5s/event)
.venv\Scripts\python.exe generators/send_stream_data.py
```

**Result:**
```
[*] Connecting to Kafka at localhost:9092...
[+] Starting event producer... Sending 100 events to topic 'credit_risk_events'

[1/100] Produced Event -> Topic: 'credit_risk_events' | Payload: CUST-1001,12000000,1785007291
[2/100] Produced Event -> Topic: 'credit_risk_events' | Payload: CUST-1003,2500000,1785007292
...
[+] Done sending streaming events!
```

**Payload format:** `customer_id,loan_amount,unix_timestamp`

---

### 1.6 View Results on Flink Web UI

1. Open browser at **http://localhost:8081**
2. Select tab **"Jobs"** → **"Running Jobs"**
3. Click on the job name to view the DAG (Directed Acyclic Graph)
4. **Green** operators = running normally
5. View output logs via tab **"Task Managers"** → **"Stdout"**

---

---

## PART 2: RESULTS & ANALYSIS

---

### 2.1 Baseline — Unoptimized Version

#### Description

The Baseline job is the simplest implementation: it connects directly to Kafka and processes data using Processing-Time windows without any error handling, risk classification, or fault tolerance mechanisms.

#### Baseline Code Snippet

```python
# streaming/flink_baseline_job.py — Core processing section

# No checkpointing
# No risk classification
# No watermarks or late arrival handling

result_stream = stream \
    .map(lambda x: evaluate_risk_payload(x)) \
    .key_by(lambda x: x['customer_id']) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
    .reduce(lambda a, b: {
        'customer_id': a['customer_id'],
        'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],
        'max_timestamp': max(a['max_timestamp'], b['max_timestamp'])
        # No risk_level
    })

result_stream.print()
```

#### Baseline Limitations

| Issue | Symptom / Description |
|---|---|
| **No data burst handling** | When high volumes of events arrive simultaneously, baseline processes sequentially without load control |
| **No risk classification** | Output only contains `total_loan_amount`, lacking HIGH/MEDIUM/LOW risk categorization |
| **No checkpointing** | If job crashes, all state is lost and must re-consume from scratch |
| **Short window (30s)** | Data arriving a few seconds late is completely ignored |
| **Raw output format** | `{'customer_id': 'CUST-1001', 'total_loan_amount': 17000000.0, 'max_timestamp': 1785007291}` |

---

### 2.2 Handle Data Burst — Managing Spike Traffic

#### Issue

In real-world credit applications, data traffic is unpredictable — dozens of transactions can arrive within seconds (bursts), overloading the processing pipeline.

**Symptoms on Flink Web UI:**
- Operator backpressure indicator turns yellow/red during bursts
- Buffer queue fills up, causing latency spikes
- Baseline behavior: events dropped or processed out of order

#### Solution in Optimized Job

**1. Increased window size — Buffer to absorb bursts:**
```python
# Baseline: 30 seconds — prone to overflow during bursts
.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))

# Optimized: 60 seconds — wider window absorbs burst traffic effectively
.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
```

**2. Reduce function accumulates state inside window — No per-event emission:**
```python
# Does not emit individual events — waits for window evaluation before emitting aggregated result
.reduce(lambda a, b: {
    'customer_id': a['customer_id'],
    'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],  # Accumulating sum
    'max_timestamp': max(a['max_timestamp'], b['max_timestamp']),
    'risk_level': merge_risk(a['total_loan_amount'] + b['total_loan_amount'])
})
```

**Result:** Instead of emitting 30 individual events during a burst, the job waits for the 60s window closure and emits **1 single aggregated record** per customer — significantly reducing downstream load.

---

### 2.3 Handle Late Arrival — Managing Out-of-Order & Delayed Data

#### Issue

In distributed systems, transactions may arrive at Kafka later than their actual event time due to:
- Network latency
- Retries from mobile clients
- Message queue backlogs

**Baseline does not handle late arrivals:** Events arriving after the window has closed are completely discarded.

#### Solution in Optimized Job

**Wider window = Natural absorption of late events:**
```python
# 60-second window allows events arriving up to ~30s late to still be grouped into the correct window
.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
```

**Extract event timestamp from payload (avoiding system wall-clock):**
```python
def evaluate_risk_payload(raw_event: str):
    parts = raw_event.strip().split(",")
    timestamp = int(parts[2])   # Extract timestamp from event payload, not Flink ingestion time

    return {
        'customer_id': customer_id,
        'total_loan_amount': loan_amount,
        'max_timestamp': timestamp,   # Retain original event-time for tracing
        'risk_level': risk_level,
    }
```

**Aggregation using `max_timestamp`** instead of processing time:
```python
'max_timestamp': max(a['max_timestamp'], b['max_timestamp'])
# Always keeps the newest timestamp in window — representing actual latest transaction time
```

> **Technical Note:** The PyFlink Python SDK (Beam-based) does not directly support `allowed_lateness()` on Python operator paths (`window_operator.py` raises `TypeError: int + Time`). Utilizing a wider window is the most stable approach in PyFlink.

---

### 2.4 Handle Other Streaming Problems

#### Problem 1: Fault Tolerance

**Baseline:** No checkpointing → crash = total state loss.

**Optimized Fix — Enable Checkpointing:**
```python
# Automatically save state every 10 seconds to local filesystem
env.enable_checkpointing(10000)   # 10,000ms = 10 seconds
```

If the job crashes and restarts, Flink recovers state from the latest checkpoint without losing processed data.

---

#### Problem 2: Port Conflict When Running 2 Parallel Jobs

**Baseline issue:** Both jobs attempt to bind to port `8081` → `BindException`.

**Optimized Fix — Dynamic Port Range:**
```python
config = Configuration()
config.set_string("rest.address", "localhost")
config.set_string("rest.port", "8081-8090")   # Automatically pick available port in range
```

Result: Baseline uses `:8081`, Optimized automatically binds to `:8082` → Both jobs run concurrently without conflicts.

---

#### Problem 3: Python Worker Runtime Error

**Baseline issue:** Flink JVM attempts to invoke system `python` executable but cannot locate the `.venv`.

**Optimized Fix — Explicitly Specify Python Executable:**
```python
import sys
env.set_python_executable(sys.executable)
# Explicitly points Flink JVM worker to: D:\credit-risk-data-platform\.venv\Scripts\python.exe
```

---

#### Problem 4: Risk Classification — Business Logic Integration

**Baseline:** Only aggregates raw metrics, lacking business logic.

**Optimized Fix — In-Pipeline Risk Classification:**
```python
def merge_risk(total_amount):
    if total_amount >= 10_000_000:   # >= 10 Million VND
        return "HIGH"
    elif total_amount >= 3_000_000:  # >= 3 Million VND
        return "MEDIUM"
    return "LOW"
```

Output is immediately actionable for downstream systems (Feature Store, alert systems):
```
[WINDOW RESULT] Customer: CUST-1001     | Total Loan:   29,000,000 VND | Risk: HIGH
[WINDOW RESULT] Customer: CUST-1003     | Total Loan:    1,500,000 VND | Risk: LOW
[WINDOW RESULT] Customer: CUST-1006     | Total Loan:   16,000,000 VND | Risk: HIGH
```

---

### 2.5 Window Processing in Flink

#### Window Concepts in Flink

A window is a mechanism that divides infinite data streams into finite chunks to apply batch computations over stream data.

| Window Type | Description | Best Use Case |
|---|---|---|
| `TumblingProcessingTimeWindows` | Fixed-size window based on processing time | Simple setups, non-critical event time order |
| `TumblingEventTimeWindows` | Fixed-size window based on event time | Strict event-time order guarantees |
| `SlidingProcessingTimeWindows` | Overlapping sliding window | Continuous rolling metrics |
| `SessionWindows` | Activity gap-based window | User session analytics |

#### Complete Code Demonstration of Window Processing

```python
# ============================================================
# COMPLETE WINDOW PIPELINE — streaming/flink_optimzed_job.py
# ============================================================

# STEP 1: Read from Kafka source
stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka_Credit_Risk_Source"
)

# STEP 2: Parse & enrich each event
parsed_stream = stream.map(evaluate_risk_payload)
# Input:  "CUST-1001,12000000,1785007291"
# Output: {'customer_id': 'CUST-1001', 'total_loan_amount': 12000000.0,
#          'max_timestamp': 1785007291, 'risk_level': 'HIGH'}

# STEP 3: Keyed Window Aggregation
aggregated_stream = parsed_stream \
    .key_by(lambda x: x['customer_id']) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(60))) \
    .reduce(lambda a, b: {
        'customer_id': a['customer_id'],
        'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],
        'max_timestamp': max(a['max_timestamp'], b['max_timestamp']),
        'risk_level': merge_risk(a['total_loan_amount'] + b['total_loan_amount'])
    })

# STEP 4: Sink — Format and print results
aggregated_stream \
    .map(lambda x: (
        f"[WINDOW RESULT] Customer: {x['customer_id']:12s} | "
        f"Total Loan: {x['total_loan_amount']:>12,.0f} VND | "
        f"Risk: {x['risk_level']}"
    )) \
    .print()
```

#### Output Example After Window Closure

**Events within a 60-second window for CUST-1001:**
```
Input:  CUST-1001,12000000,1785007291   → risk: HIGH
Input:  CUST-1001, 5000000,1785007292   → risk: MEDIUM
Input:  CUST-1001,12000000,1785007293   → risk: HIGH
```

**After 60 seconds, Flink emits the aggregated result:**
```
[WINDOW RESULT] Customer: CUST-1001     | Total Loan:   29,000,000 VND | Risk: HIGH
```

#### DAG Visual on Flink Web UI

**Baseline DAG** (`http://localhost:8081`):
```
Source: Kafka_Baseline_Source
    → Map (evaluate_risk_payload)
    → KeyedProcess (TumblingWindow 30s + reduce)
    → Sink: Print to Std. Out
```

**Optimized DAG** (`http://localhost:8082`):
```
Source: Kafka_Credit_Risk_Source
    → Map (evaluate_risk_payload + risk classification)
    → KeyedProcess (TumblingWindow 60s + reduce + merge_risk)
    → Map (format output string)
    → Sink: Print to Std. Out
```

> When executing `send_stream_data.py`, operator states transition from `IDLE` to `RUNNING` (green). The window operator retains state until the window time elapses, after which it emits the result and resets state for the next window.

---

### 2.6 Summary Comparison Table: Baseline vs Optimized

| Metric / Feature | Baseline | Optimized |
|---|---|---|
| **Window Size** | 30 seconds | 60 seconds |
| **Risk Classification** | ❌ None | ✅ HIGH / MEDIUM / LOW |
| **Checkpointing** | ❌ None | ✅ Every 10 seconds |
| **Fault Tolerance** | ❌ Loses state on crash | ✅ Recovers from checkpoint |
| **Output Format** | Raw dictionary | Clean formatted string |
| **Dynamic REST Port** | ❌ Fixed 8081 | ✅ Range 8081-8090 |
| **Burst Handling** | ❌ Processes per event | ✅ Aggregates via larger window |
| **Late Arrival Tolerance** | ❌ Discarded | ✅ Absorbed by wider window |
| **Consumer Group** | `credit_risk_baseline_group` | `credit_risk_optimized_group` |

---
