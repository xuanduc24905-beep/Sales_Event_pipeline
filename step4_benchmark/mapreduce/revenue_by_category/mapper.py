#!/usr/bin/env python3
"""Mapper: JSON Lines → category\ttotal_amount (completed, amount>0 only)"""
import sys, json, hashlib

CATEGORIES = ["electronics","clothing","home_garden","sports","beauty",
              "books","food_drink","toys","automotive","health"]

def get_category(seller_id):
    return CATEGORIES[int(hashlib.md5(seller_id.encode()).hexdigest()[:8], 16) % 10]

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        rec = json.loads(line)
        if rec.get("status") != "completed": continue
        amt = rec.get("total_amount")
        if amt is None or float(amt) <= 0: continue
        print(f"{get_category(rec.get('seller_id',''))}\t{float(amt)}")
    except Exception: continue
