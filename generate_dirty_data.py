#!/usr/bin/env python3
"""
Generate network_logs_dirty.csv with specified data quality issues:
- ~3% duplicate records (by timestamp + src_ip + dst_ip + port)
- ~2% missing values in selected columns
- Invalid timestamps
- Timestamps outside allowed range
- Invalid IPs, ports, negative bytes
- NULL status values
"""

import csv
import random
import ipaddress
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Configuration
NUM_RECORDS = 10000
DUPLICATE_RATE = 0.03
MISSING_RATE = 0.02
INVALID_TIMESTAMP_RATE = 0.015
OUT_OF_RANGE_RATE = 0.01
INVALID_IP_RATE = 0.01
INVALID_PORT_RATE = 0.01
NEGATIVE_BYTES_RATE = 0.005
NULL_STATUS_RATE = 0.02

# Allowed timestamp range (UTC)
ALLOWED_START = datetime(2024, 1, 1, 0, 0, 0)
ALLOWED_END = datetime(2024, 12, 31, 23, 59, 59)

# Protocols and statuses
PROTOCOLS = ['TCP', 'UDP', 'ICMP', 'HTTP', 'HTTPS', 'DNS', 'SSH', 'FTP']
STATUSES = ['SUCCESS', 'TIMEOUT', 'RESET', 'REFUSED', 'UNREACHABLE', 'ERROR']

def random_ip():
    return str(ipaddress.IPv4Address(random.randint(1, 2**32 - 1)))

def random_private_ip():
    # Generate private IPs for more realism
    ranges = [
        (ipaddress.IPv4Address('10.0.0.0'), ipaddress.IPv4Address('10.255.255.255')),
        (ipaddress.IPv4Address('172.16.0.0'), ipaddress.IPv4Address('172.31.255.255')),
        (ipaddress.IPv4Address('192.168.0.0'), ipaddress.IPv4Address('192.168.255.255')),
    ]
    start, end = random.choice(ranges)
    return str(ipaddress.IPv4Address(random.randint(int(start), int(end))))

def random_public_ip():
    # Avoid private ranges
    while True:
        ip = ipaddress.IPv4Address(random.randint(1, 2**32 - 1))
        if not ip.is_private and not ip.is_loopback and not ip.is_multicast:
            return str(ip)

def generate_valid_record(record_id):
    timestamp = ALLOWED_START + timedelta(seconds=random.randint(0, int((ALLOWED_END - ALLOWED_START).total_seconds())))
    return {
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'src_ip': random_private_ip() if random.random() < 0.7 else random_public_ip(),
        'dst_ip': random_public_ip() if random.random() < 0.8 else random_private_ip(),
        'port': random.choice([22, 23, 25, 53, 80, 110, 143, 443, 465, 587, 993, 995, 3306, 5432, 8080, 8443] + [random.randint(1024, 65535) for _ in range(20)]),
        'bytes': random.randint(64, 10485760),  # 64 bytes to 10MB
        'protocol': random.choice(PROTOCOLS),
        'status': random.choice(STATUSES),
        'label': random.choice(['benign', 'malicious']) if random.random() < 0.1 else 'benign',
        'anomaly_type': random.choice(['port_scan', 'ddos', 'brute_force', 'data_exfil', '']) if random.random() < 0.05 else ''
    }

def inject_issues(records):
    """Inject data quality issues into records"""
    total = len(records)
    num_duplicates = int(total * DUPLICATE_RATE)
    num_missing = int(total * MISSING_RATE)
    num_invalid_ts = int(total * INVALID_TIMESTAMP_RATE)
    num_oob_ts = int(total * OUT_OF_RANGE_RATE)
    num_invalid_ip = int(total * INVALID_IP_RATE)
    num_invalid_port = int(total * INVALID_PORT_RATE)
    num_neg_bytes = int(total * NEGATIVE_BYTES_RATE)
    num_null_status = int(total * NULL_STATUS_RATE)

    indices = list(range(total))
    random.shuffle(indices)
    idx = 0

    # 1. Duplicates: copy existing records
    for _ in range(num_duplicates):
        if idx >= len(indices): break
        src_idx = random.randint(0, total - 1)
        records.append(records[src_idx].copy())
        idx += 1

    # 2. Missing values
    missing_cols = ['timestamp', 'src_ip', 'dst_ip', 'port', 'bytes', 'status']
    for _ in range(num_missing):
        if idx >= len(indices): break
        i = indices[idx]
        col = random.choice(missing_cols)
        records[i][col] = ''
        idx += 1

    # 3. Invalid timestamps
    invalid_formats = [
        '2024-13-45 25:70:70',  # completely invalid
        'not-a-timestamp',       # text
        '2024/01/01 12:00:00',   # wrong format
        '01-01-2024 12:00:00',   # wrong format
        '',                      # empty
        '2024-02-30 12:00:00',   # invalid date
    ]
    for _ in range(num_invalid_ts):
        if idx >= len(indices): break
        i = indices[idx]
        records[i]['timestamp'] = random.choice(invalid_formats)
        idx += 1

    # 4. Out of range timestamps (before 2024 or after 2024)
    for _ in range(num_oob_ts):
        if idx >= len(indices): break
        i = indices[idx]
        # Either far past or far future
        if random.random() < 0.5:
            ts = ALLOWED_START - timedelta(days=random.randint(365, 3650))
        else:
            ts = ALLOWED_END + timedelta(days=random.randint(365, 3650))
        records[i]['timestamp'] = ts.strftime('%Y-%m-%d %H:%M:%S')
        idx += 1

    # 5. Invalid IPs
    invalid_ips = ['999.999.999.999', '192.168.1', '192.168.1.1.1', 'not.an.ip', '300.200.100.50', '', '192.168.1.256']
    for _ in range(num_invalid_ip):
        if idx >= len(indices): break
        i = indices[idx]
        if random.random() < 0.5:
            records[i]['src_ip'] = random.choice(invalid_ips)
        else:
            records[i]['dst_ip'] = random.choice(invalid_ips)
        idx += 1

    # 6. Invalid ports
    for _ in range(num_invalid_port):
        if idx >= len(indices): break
        i = indices[idx]
        records[i]['port'] = random.choice([-1, 0, 65536, 99999, 'abc', ''])
        idx += 1

    # 7. Negative bytes
    for _ in range(num_neg_bytes):
        if idx >= len(indices): break
        i = indices[idx]
        records[i]['bytes'] = random.randint(-10000, -1)
        idx += 1

    # 8. NULL status
    for _ in range(num_null_status):
        if idx >= len(indices): break
        i = indices[idx]
        records[i]['status'] = ''
        idx += 1

    return records

def main():
    print(f"Generating {NUM_RECORDS} base records...")
    records = [generate_valid_record(i) for i in range(NUM_RECORDS)]

    print("Injecting data quality issues...")
    records = inject_issues(records)

    # Shuffle final records
    random.shuffle(records)

    # Write CSV
    fieldnames = ['timestamp', 'src_ip', 'dst_ip', 'port', 'bytes', 'protocol', 'status', 'label', 'anomaly_type']
    output_path = 'network_logs_dirty.csv'
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Written {len(records)} records to {output_path}")
    
    # Print stats
    print("\n--- Data Quality Summary ---")
    print(f"Total records: {len(records)}")
    print(f"Expected duplicates (~3%): {int(NUM_RECORDS * DUPLICATE_RATE)}")
    print(f"Expected missing values (~2%): {int(NUM_RECORDS * MISSING_RATE)}")
    print(f"Expected invalid timestamps (~1.5%): {int(NUM_RECORDS * INVALID_TIMESTAMP_RATE)}")
    print(f"Expected out-of-range timestamps (~1%): {int(NUM_RECORDS * OUT_OF_RANGE_RATE)}")
    print(f"Expected invalid IPs (~1%): {int(NUM_RECORDS * INVALID_IP_RATE)}")
    print(f"Expected invalid ports (~1%): {int(NUM_RECORDS * INVALID_PORT_RATE)}")
    print(f"Expected negative bytes (~0.5%): {int(NUM_RECORDS * NEGATIVE_BYTES_RATE)}")
    print(f"Expected NULL status (~2%): {int(NUM_RECORDS * NULL_STATUS_RATE)}")

if __name__ == '__main__':
    main()