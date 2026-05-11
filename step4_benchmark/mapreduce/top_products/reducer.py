#!/usr/bin/env python3
"""Reducer: count orders per product_id → emit top 10

NOTE: Collect ALL products vào memory — chỉ đúng với 1 reducer duy nhất.
      Spark dùng partial agg per partition + final sort → không cần bottleneck này.
"""
import sys, heapq

counts = {}
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2: continue
    try:
        pid, n = parts[0], int(parts[1])
        counts[pid] = counts.get(pid, 0) + n
    except ValueError: continue

for rank, (pid, cnt) in enumerate(heapq.nlargest(10, counts.items(), key=lambda x: x[1]), 1):
    print(f"{rank}\t{pid}\t{cnt}")
