#!/usr/bin/env python3
"""Mapper: JSON Lines → year_month\ttotal_amount (completed, amount>0 only)

NOTE: Job này chỉ tính sum/count per month.
      MoM growth % cần Job 2 riêng vì MapReduce không có Window function.
      Spark xử lý trong 1 DAG với Window.lag() — đây là case study quan trọng nhất.
"""
import sys, json

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        rec = json.loads(line)
        if rec.get("status") != "completed": continue
        amt = rec.get("total_amount")
        if amt is None or float(amt) <= 0: continue
        ym = rec.get("year_month") or str(rec.get("order_date",""))[:7]
        if ym: print(f"{ym}\t{float(amt)}")
    except Exception: continue
