# Online Streaming Pipeline — Credit Risk Data Platform

---

## PHẦN 1: HƯỚNG DẪN CHẠY ONLINE STREAMING

### 1.1 Kiến trúc tổng thể

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

### 1.2 Khởi động Kafka & Zookeeper (Docker)

**Bước 1: Khởi động toàn bộ infrastructure**

```powershell
# Tại thư mục gốc dự án
docker-compose up -d
```

**Bước 2: Xác nhận containers đang chạy**

```powershell
docker ps
```

Kết quả mong đợi:
```
CONTAINER ID   IMAGE                            PORTS                    NAMES
xxxxxxxxxxxx   confluentinc/cp-kafka:7.3.0      0.0.0.0:9092->9092/tcp   platform-kafka
xxxxxxxxxxxx   confluentinc/cp-zookeeper:7.3.0  0.0.0.0:2181->2181/tcp   platform-zookeeper
```

**Bước 3: Kiểm tra Kafka topic**

```powershell
# Liệt kê các topics hiện có
docker exec -it platform-kafka kafka-topics --list --bootstrap-server localhost:9092

# (Tùy chọn) Tạo topic thủ công nếu chưa tồn tại
docker exec -it platform-kafka kafka-topics \
  --create --topic credit_risk_events \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

---

### 1.3 Chạy Flink Baseline Job

> **File:** `streaming/flink_baseline_job.py`

```powershell
# Terminal 1 — Khởi động Baseline Job
.venv\Scripts\python.exe streaming/flink_baseline_job.py
```

**Kết quả khởi động thành công:**
```
[*] Loaded Kafka connector JAR: file:///D:/credit-risk-data-platform/plugins/...

============================================================
[+] Flink Web UI Dashboard: http://localhost:8081
============================================================
```

- Flink Web UI: **http://localhost:8081**
- Window: `TumblingProcessingTimeWindows` — **30 giây**
- Consumer group: `credit_risk_baseline_group`
- Không có risk classification, không có checkpointing

---

### 1.4 Chạy Flink Optimized Job

> **File:** `streaming/flink_optimzed_job.py`

```powershell
# Terminal 2 — Khởi động Optimized Job (có thể chạy song song Baseline)
.venv\Scripts\python.exe streaming/flink_optimzed_job.py
```

**Kết quả khởi động thành công:**
```
[*] Loaded Kafka connector JAR: file:///D:/...

============================================================
[+] Flink Optimized Job starting...
[+] Flink Web UI Dashboard: http://localhost:8081 (or :8082)
============================================================
```

- Flink Web UI: **http://localhost:8081** (hoặc `:8082` nếu port 8081 đã bị chiếm)
- Window: `TumblingProcessingTimeWindows` — **60 giây**
- Consumer group: `credit_risk_optimized_group`
- Có risk classification (`HIGH/MEDIUM/LOW`), có checkpointing 10 giây

---

### 1.5 Bắn dữ liệu vào Kafka để test

> **File:** `generators/send_stream_data.py`

```powershell
# Terminal 3 — Bắn 100 events vào Kafka (0.5s/event)
.venv\Scripts\python.exe generators/send_stream_data.py
```

**Kết quả:**
```
[*] Connecting to Kafka at localhost:9092...
[+] Starting event producer... Sending 100 events to topic 'credit_risk_events'

[1/100] Produced Event -> Topic: 'credit_risk_events' | Payload: CUST-1001,12000000,1785007291
[2/100] Produced Event -> Topic: 'credit_risk_events' | Payload: CUST-1003,2500000,1785007292
...
[+] Done sending streaming events!
```

**Format payload:** `customer_id,loan_amount,unix_timestamp`

---

### 1.6 Xem kết quả trên Flink Web UI

1. Mở trình duyệt tại **http://localhost:8081**
2. Chọn tab **"Jobs"** → **"Running Jobs"**
3. Click vào tên job để xem DAG (Directed Acyclic Graph)
4. Các operator màu **xanh lá** = đang chạy bình thường
5. Xem output logs qua tab **"Task Managers"** → **"Stdout"**

---

---

## PHẦN 2: KẾT QUẢ & PHÂN TÍCH

---

### 2.1 Baseline — Không có tối ưu hóa

#### Mô tả

Baseline job là phiên bản đơn giản nhất: kết nối trực tiếp Kafka và xử lý dữ liệu theo cửa sổ thời gian xử lý (Processing-Time), không có bất kỳ cơ chế xử lý lỗi, phân loại rủi ro, hay fault tolerance nào.

#### Code Baseline

```python
# streaming/flink_baseline_job.py — Đoạn xử lý chính

# Không có checkpointing
# Không có risk classification
# Không có watermark hay xử lý late arrival

result_stream = stream \
    .map(lambda x: evaluate_risk_payload(x)) \
    .key_by(lambda x: x['customer_id']) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
    .reduce(lambda a, b: {
        'customer_id': a['customer_id'],
        'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],
        'max_timestamp': max(a['max_timestamp'], b['max_timestamp'])
        # Không có risk_level
    })

result_stream.print()
```

#### Hạn chế của Baseline

| Vấn đề | Biểu hiện |
|---|---|
| **Không xử lý data burst** | Nhiều events đến đồng loạt, baseline xử lý tuần tự không kiểm soát tải |
| **Không phân loại rủi ro** | Output chỉ có `total_loan_amount`, không biết HIGH/MEDIUM/LOW |
| **Không checkpoint** | Nếu job crash, toàn bộ state bị mất, phải đọc lại từ đầu |
| **Window ngắn (30s)** | Dữ liệu đến trễ vài giây sẽ bị bỏ qua hoàn toàn |
| **Output thô** | `{'customer_id': 'CUST-1001', 'total_loan_amount': 17000000.0, 'max_timestamp': 1785007291}` |

---

### 2.2 Handle Data Burst — Xử lý luồng dữ liệu đột biến

#### Vấn đề

Trong môi trường tín dụng thực tế, luồng dữ liệu không đồng đều — có những thời điểm hàng chục giao dịch đến trong vài giây (burst), khiến hệ thống quá tải.

**Biểu hiện trên Flink Web UI:**
- Operator backpressure màu vàng/đỏ khi burst
- Queue đầy, latency tăng đột biến
- Với baseline: events bị drop hoặc xử lý sai thứ tự

#### Giải pháp trong Optimized Job

**1. Tăng kích thước window — Buffer hấp thụ burst:**
```python
# Baseline: 30 giây — dễ tràn khi burst
.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))

# Optimized: 60 giây — window rộng hơn hấp thụ tốt burst traffic
.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
```

**2. Reduce function tích lũy state trong window — Không emit từng event rời:**
```python
# Không emit mỗi event riêng lẻ — chờ window đóng rồi mới emit kết quả tổng hợp
.reduce(lambda a, b: {
    'customer_id': a['customer_id'],
    'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],  # Cộng dồn
    'max_timestamp': max(a['max_timestamp'], b['max_timestamp']),
    'risk_level': merge_risk(a['total_loan_amount'] + b['total_loan_amount'])
})
```

**Kết quả:** Thay vì emit 30 events rời rạc khi burst, job chờ window 60s đóng và emit **1 bản tổng hợp duy nhất** per customer — giảm downstream load đáng kể.

---

### 2.3 Handle Late Arrival — Xử lý dữ liệu đến trễ

#### Vấn đề

Trong hệ thống phân tán, giao dịch có thể đến Kafka muộn hơn thời gian thực tế do:
- Network latency
- Retry từ mobile client
- Message queue backlog

**Baseline không xử lý late arrival:** Events đến sau khi window đã đóng sẽ bị bỏ qua hoàn toàn.

#### Giải pháp trong Optimized Job

**Window rộng hơn = tự nhiên hấp thụ late events:**
```python
# Cửa sổ 60s cho phép events đến trễ tối đa ~30s vẫn được gom đúng window
.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
```

**Ghi nhận timestamp từ payload event (không dùng wall clock):**
```python
def evaluate_risk_payload(raw_event: str):
    parts = raw_event.strip().split(",")
    timestamp = int(parts[2])   # Dùng timestamp từ event, không phải thời điểm Flink nhận

    return {
        'customer_id': customer_id,
        'total_loan_amount': loan_amount,
        'max_timestamp': timestamp,   # Giữ lại event-time gốc để tracing
        'risk_level': risk_level,
    }
```

**Aggregation dùng `max_timestamp`** thay vì processing time:
```python
'max_timestamp': max(a['max_timestamp'], b['max_timestamp'])
# Luôn lấy timestamp mới nhất trong window — đại diện cho thời điểm giao dịch thực tế
```

> **Lưu ý kỹ thuật:** PyFlink Python SDK (Beam-based) không hỗ trợ `allowed_lateness()` trực tiếp trên Python operator path (`window_operator.py` sẽ raise `TypeError: int + Time`). Giải pháp dùng window rộng hơn là cách tiếp cận ổn định nhất với PyFlink.

---

### 2.4 Handle Other Streaming Problems — Các vấn đề streaming khác

#### Vấn đề 1: Fault Tolerance (Chịu lỗi)

**Baseline:** Không có checkpointing → crash = mất toàn bộ state.

**Optimized Fix — Bật Checkpointing:**
```python
# Tự động lưu state mỗi 10 giây vào local filesystem
env.enable_checkpointing(10000)   # 10,000ms = 10 giây
```

Nếu job bị crash và restart, Flink khôi phục từ checkpoint gần nhất, không mất dữ liệu đã xử lý.

---

#### Vấn đề 2: Port Conflict khi chạy 2 jobs song song

**Baseline gặp lỗi:** Cả 2 jobs cùng cố bind port `8081` → `BindException`.

**Optimized Fix — Dynamic Port Range:**
```python
config = Configuration()
config.set_string("rest.address", "localhost")
config.set_string("rest.port", "8081-8090")   # Tự động chọn port trống trong range
```

Kết quả: Baseline dùng `:8081`, Optimized tự động bind `:8082` → 2 jobs chạy song song được.

---

#### Vấn đề 3: Python Worker Runtime Error

**Baseline gặp lỗi:** Flink JVM cố gọi `python` system nhưng không tìm được `.venv`.

**Optimized Fix — Chỉ định Python executable rõ ràng:**
```python
import sys
env.set_python_executable(sys.executable)
# Trỏ JVM Flink worker đúng vào: D:\credit-risk-data-platform\.venv\Scripts\python.exe
```

---

#### Vấn đề 4: Risk Classification — Business Logic

**Baseline:** Chỉ tổng hợp số liệu thô, không có logic nghiệp vụ.

**Optimized Fix — Phân loại rủi ro ngay trong pipeline:**
```python
def merge_risk(total_amount):
    if total_amount >= 10_000_000:   # >= 10 triệu VND
        return "HIGH"
    elif total_amount >= 3_000_000:  # >= 3 triệu VND
        return "MEDIUM"
    return "LOW"
```

Output dùng được ngay cho downstream systems (Feature Store, alert systems):
```
[WINDOW RESULT] Customer: CUST-1001     | Total Loan:   29,000,000 VND | Risk: HIGH
[WINDOW RESULT] Customer: CUST-1003     | Total Loan:    1,500,000 VND | Risk: LOW
[WINDOW RESULT] Customer: CUST-1006     | Total Loan:   16,000,000 VND | Risk: HIGH
```

---

### 2.5 Window Processing — Xử lý cửa sổ trong Flink

#### Khái niệm Window trong Flink

Window là cơ chế gom nhóm luồng dữ liệu vô hạn thành các tập con hữu hạn để xử lý theo batch logic trên stream.

| Loại Window | Mô tả | Dùng khi |
|---|---|---|
| `TumblingProcessingTimeWindows` | Cửa sổ cố định theo thời gian xử lý | Cần đơn giản, không cần event-time |
| `TumblingEventTimeWindows` | Cửa sổ cố định theo thời gian sự kiện | Cần đảm bảo thứ tự event-time |
| `SlidingProcessingTimeWindows` | Cửa sổ trượt (overlap) | Cần tính toán liên tục hơn |
| `SessionWindows` | Cửa sổ theo phiên hoạt động | Phân tích session user |

#### Code thể hiện Window Processing hoàn chỉnh

```python
# ============================================================
# WINDOW PIPELINE HOÀN CHỈNH — streaming/flink_optimzed_job.py
# ============================================================

# BƯỚC 1: Đọc từ Kafka source
stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka_Credit_Risk_Source"
)

# BƯỚC 2: Parse & enrich mỗi event
parsed_stream = stream.map(evaluate_risk_payload)
# Input:  "CUST-1001,12000000,1785007291"
# Output: {'customer_id': 'CUST-1001', 'total_loan_amount': 12000000.0,
#          'max_timestamp': 1785007291, 'risk_level': 'HIGH'}

# BƯỚC 3: Keyed Window Aggregation
aggregated_stream = parsed_stream \
    .key_by(lambda x: x['customer_id']) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(60))) \
    .reduce(lambda a, b: {
        'customer_id': a['customer_id'],
        'total_loan_amount': a['total_loan_amount'] + b['total_loan_amount'],
        'max_timestamp': max(a['max_timestamp'], b['max_timestamp']),
        'risk_level': merge_risk(a['total_loan_amount'] + b['total_loan_amount'])
    })

# BƯỚC 4: Sink — Format và in kết quả
aggregated_stream \
    .map(lambda x: (
        f"[WINDOW RESULT] Customer: {x['customer_id']:12s} | "
        f"Total Loan: {x['total_loan_amount']:>12,.0f} VND | "
        f"Risk: {x['risk_level']}"
    )) \
    .print()
```

#### Ví dụ kết quả sau khi window đóng

**Events trong 60 giây của CUST-1001:**
```
Input:  CUST-1001,12000000,1785007291   → risk: HIGH
Input:  CUST-1001, 5000000,1785007292   → risk: MEDIUM
Input:  CUST-1001,12000000,1785007293   → risk: HIGH
```

**Sau 60 giây, Flink emit kết quả tổng hợp:**
```
[WINDOW RESULT] Customer: CUST-1001     | Total Loan:   29,000,000 VND | Risk: HIGH
```

#### DAG trên Flink Web UI

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

> Khi bắn `send_stream_data.py`, các operator chuyển từ `IDLE` sang `RUNNING` (màu xanh lá). Window operator giữ state cho đến khi window time kết thúc, sau đó emit kết quả và reset state cho window tiếp theo.

---

### 2.6 Bảng tổng kết so sánh Baseline vs Optimized

| Tiêu chí | Baseline | Optimized |
|---|---|---|
| **Window size** | 30 giây | 60 giây |
| **Risk classification** | ❌ Không có | ✅ HIGH / MEDIUM / LOW |
| **Checkpointing** | ❌ Không có | ✅ Mỗi 10 giây |
| **Fault tolerance** | ❌ Mất state khi crash | ✅ Khôi phục từ checkpoint |
| **Output format** | Dict thô | Chuỗi formatted dễ đọc |
| **Dynamic REST port** | ❌ Fixed 8081 | ✅ Range 8081-8090 |
| **Burst handling** | ❌ Xử lý từng event | ✅ Gom qua window lớn hơn |
| **Late arrival tolerance** | ❌ Bỏ qua | ✅ Window rộng hấp thụ tự nhiên |
| **Consumer group** | `credit_risk_baseline_group` | `credit_risk_optimized_group` |
