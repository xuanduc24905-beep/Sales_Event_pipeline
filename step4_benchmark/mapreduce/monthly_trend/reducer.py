#!/usr/bin/env python3
"""Reducer: sum revenue + count orders per year_month
Output: year_month\ttotal_revenue\torder_count

NOTE: MoM growth % KHÔNG THỂ tính ở đây — reducer chỉ thấy 1 key tại 1 thời điểm.
      Cần Job 2 riêng để tính lag(1). Spark làm trong 1 DAG — không cần HDFS write trung gian.
"""
import sys

current, total, count = None, 0.0, 0
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2: continue
    try: ym, amt = parts[0], float(parts[1])
    except ValueError: continue
    if ym == current:
        total += amt; count += 1
    else:
        if current is not None: print(f"{current}\t{total:.2f}\t{count}")
        current, total, count = ym, amt, 1
if current is not None: print(f"{current}\t{total:.2f}\t{count}")
