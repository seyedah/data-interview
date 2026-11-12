# Data Dictionary: network_logs_dirty.csv

## File Metadata
| Property | Value |
|----------|-------|
| **File Name** | `network_logs_dirty.csv` |
| **Format** | CSV |
| **Encoding** | UTF-8 |
| **Delimiter** | Comma (`,`) |
| **Timezone** | UTC |
| **Total Records** | 10,300 (including ~300 duplicates) |
| **Unique Records (est.)** | ~10,000 |
| **Header Row** | Yes |

---

## Schema

| Column | Type | Required | Description | Constraints / Valid Values |
|--------|------|----------|-------------|----------------------------|
| `timestamp` | STRING (datetime) | ✅ Yes | Event timestamp in UTC | Must parse as `YYYY-MM-DD HH:MM:SS`; must fall within **2024-01-01 00:00:00** to **2024-12-31 23:59:59** (inclusive) |
| `src_ip` | STRING (IPv4) | ✅ Yes | Source IP address | Valid IPv4 address (e.g., `192.168.1.1`, `10.0.0.1`, `8.8.8.8`) |
| `dst_ip` | STRING (IPv4) | ✅ Yes | Destination IP address | Valid IPv4 address |
| `port` | INTEGER | ✅ Yes | Destination port | Range: **1 – 65535** |
| `bytes` | INTEGER | No | Data volume transferred (bytes) | Must be **≥ 0** if present; `NULL`/empty allowed |
| `protocol` | STRING | No | Communication protocol | Observed: `TCP`, `UDP`, `ICMP`, `HTTP`, `HTTPS`, `DNS`, `SSH`, `FTP`, `ICMP` |
| `status` | STRING | No | Connection status | Observed: `SUCCESS`, `TIMEOUT`, `RESET`, `REFUSED`, `UNREACHABLE`, `ERROR`, `UNKNOWN` (after cleaning), empty/`NULL` |
| `label` | STRING | No | Traffic classification | `benign`, `malicious` |
| `anomaly_type` | STRING | No | Type of anomaly (if malicious) | `port_scan`, `ddos`, `brute_force`, `data_exfil`, empty |

---

## Data Contract Rules (from assignment)

1. **Timestamp** must be valid UTC and within allowed range: **2024-01-01 00:00:00 → 2024-12-31 23:59:59**
2. **Composite key for deduplication**: `timestamp + src_ip + dst_ip + port`
3. `bytes` can be `NULL` but **never negative**
4. Empty `status` → set to `UNKNOWN` (not an error event)
5. Records with missing/invalid required fields (`timestamp`, `src_ip`, `dst_ip`, `port`) → **Quarantine**
6. **Pipeline stop condition**: If >5% of input records have invalid/missing timestamps → STOP

---

## Known Data Quality Issues (Injected)

| Issue | Approx. Count | Rate | Example Values |
|-------|---------------|------|----------------|
| **Duplicate records** (by composite key) | ~288 | ~2.8% | Exact copies of valid rows |
| **Missing timestamp** | ~190 | ~1.8% | Empty string, `not-a-timestamp`, `2024-02-30 12:00:00`, `2024/01/01 12:00:00`, `01-01-2024 12:00:00` |
| **Out-of-range timestamp** | ~100 | ~1.0% | `2019-05-16`, `2021-01-09`, `2029-01-01`, `2030-05-01`, `2033-04-29` |
| **Missing src_ip / dst_ip** | ~166 | ~1.6% | Empty string, `not.an.ip`, `192.168.1` (incomplete) |
| **Invalid IP format** | ~166 | ~1.6% | `999.999.999.999`, `300.200.100.50`, `192.168.1.256` |
| **Invalid port** | ~125 | ~1.2% | `-1`, `0`, `65536`, `99999`, `abc`, empty |
| **Negative bytes** | ~50 | ~0.5% | `-7461`, `-359`, `-5290`, `-9776`, `-4166` |
| **NULL/empty status** | ~234 | ~2.3% | Empty string |
| **Missing bytes** | ~454 (across cols) | ~4.4% | Empty string |
| **Other malformed** | Various | — | `not-a-timestamp`, `abc` in port, `192.168.1.256`, `999.999.999.999` |

> **Note**: Total invalid + out-of-range timestamps = ~290 (2.8%) — **below the 5% pipeline stop threshold**.

---

## Cleaning Policy (Per Assignment)

| Condition | Action |
|-----------|--------|
| Invalid timestamp | Quarantine (`INVALID_TIMESTAMP`) |
| Timestamp out of range | Quarantine (`TIMESTAMP_OUT_OF_RANGE`) |
| Missing `src_ip` or `dst_ip` | Quarantine (`MISSING_REQUIRED_FIELD`) |
| Invalid IP format | Quarantine (`INVALID_IP`) |
| Invalid port | Quarantine (`INVALID_PORT`) |
| `bytes` = NULL/empty | Replace with **batch median** (non-negative) |
| `bytes` < 0 | Quarantine (`NEGATIVE_BYTES`) |
| `status` = NULL/empty | Set to `UNKNOWN` |
| Duplicate (by composite key) | Keep **1** (deterministic), quarantine rest (`DUPLICATE_RECORD`) |

---

## Aggregation Logic (Daily, per `src_ip`)

| Output Column | Calculation |
|---------------|-------------|
| `event_date` | `DATE(timestamp)` in UTC |
| `src_ip` | Group by |
| `total_events` | `COUNT(*)` |
| `total_bytes` | `SUM(bytes)` (NULLs ignored) |
| `avg_bytes` | `AVG(bytes)` (NULLs ignored) |
| `error_events` | `COUNT(*) WHERE status IN ('TIMEOUT', 'RESET')` — `UNKNOWN` excluded |

---

## Error Event Definition
- `status` ∈ (`TIMEOUT`, `RESET`) → **Error Event**
- `status` = `UNKNOWN` → **NOT** an error event

---

## Idempotency Keys

| Layer | Unique Constraint / Upsert Key |
|-------|-------------------------------|
| `raw_network_logs` | `batch_id` (file hash) — prevent re-insert of same batch |
| `stg_network_logs` | `batch_id` + composite key — re-run produces identical results |
| `rejected_network_logs` | `batch_id` + `source_record_id` — no duplicate quarantine rows |
| `mart_daily_network_stats` | `event_date` + `src_ip` — upsert on re-run |

---

## Analytical Queries (Required)

1. **Top 5 Error Sources**: `src_ip`, `total_error_events` from mart, ordered desc, limit 5
2. **Daily Traffic Ranking**: `event_date`, `src_ip`, `total_bytes`, `daily_rank` using `RANK()` or `DENSE_RANK()` partition by `event_date` order by `total_bytes` desc
3. **Weekly Trend (Optional)**: 7-day moving average of `total_events` per day