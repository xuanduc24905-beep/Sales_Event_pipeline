#!/usr/bin/env python3
"""Mapper: JSON Lines → product_id\t1 (completed orders only)

product_id = f"P{int(md5(order_id)[:8],16) % 500 + 1:05d}" — cùng logic Spark.
NOTE: Benchmark dùng numReduceTasks=1 để đảm bảo top-10 global chính xác.
      Với nhiều reducers mỗi cái ra top-10 local → cần Job 2 để merge (bottleneck).
"""
import sys, json, hashlib

def get_product_id(order_id):
    return f"P{int(hashlib.md5(order_id.encode()).hexdigest()[:8], 16) % 500 + 1:05d}"

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        rec = json.loads(line)
        if rec.get("status") != "completed": continue
        amt = rec.get("total_amount")
        if amt is None or float(amt) <= 0: continue
        oid = rec.get("order_id","")
        if oid: print(f"{get_product_id(oid)}\t1")
    except Exception: continue
