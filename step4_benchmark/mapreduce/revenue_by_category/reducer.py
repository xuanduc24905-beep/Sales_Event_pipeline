#!/usr/bin/env python3
"""Reducer: sum total_amount per category"""
import sys

current, total = None, 0.0
for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2: continue
    try: cat, amt = parts[0], float(parts[1])
    except ValueError: continue
    if cat == current:
        total += amt
    else:
        if current is not None: print(f"{current}\t{total:.2f}")
        current, total = cat, amt
if current is not None: print(f"{current}\t{total:.2f}")
