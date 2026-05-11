#!/usr/bin/env python3
"""
STEP 0 — generate_orders.py
Generate 200k fake orders (2022-2024) from 2 sources (app + web)
with different JSON formats and upload to HDFS.

Usage: python3 generate_orders.py
"""

import json
import math
import os
import random
import subprocess
import time
import uuid
from datetime import datetime, timezone, timedelta

# ── Constants ─────────────────────────────────────────────────────────────────
HDFS = "hdfs://192.168.56.10:9000"

N_APP = 120_000   # 2 part files of 60k each
N_WEB =  80_000   # 2 part files of 40k each

random.seed(42)

REGIONS = ["north", "south", "central", "west", "east"]

CATEGORIES = [
    "electronics", "clothing", "home_garden", "sports", "beauty",
    "books", "food_drink", "toys", "automotive", "health"
]

# 500 products P00001-P00500
PRODUCTS = [f"P{i:05d}" for i in range(1, 501)]

# 50000 customers C000001-C050000
CUSTOMERS = [f"C{i:06d}" for i in range(1, 50001)]

# 200 sellers: S001-S200
# S001-S060   → high_performer  (30%)
# S061-S160   → average_seller  (50%)
# S161-S200   → low_performer   (20%)
SELLERS = []
SELLER_TYPE = {}
SELLER_REGION = {}
for i in range(1, 201):
    sid = f"S{i:03d}"
    SELLERS.append(sid)
    if i <= 60:
        SELLER_TYPE[sid] = "high_performer"
    elif i <= 160:
        SELLER_TYPE[sid] = "average_seller"
    else:
        SELLER_TYPE[sid] = "low_performer"
    # region = REGIONS[(i-1) % 5]
    SELLER_REGION[sid] = REGIONS[(i - 1) % 5]

START_DATE = datetime(2022, 1, 1)
END_DATE   = datetime(2024, 12, 31, 23, 59, 59)
EPOCH_2022 = START_DATE.timestamp()

# Status weights: completed=68%, pending=15%, cancelled=10%, returned=5%, unknown=2%
STATUS_VALUES  = ["completed", "pending", "cancelled", "returned", "unknown"]
STATUS_WEIGHTS = [0.68, 0.15, 0.10, 0.05, 0.02]

# Status aliases for APP (abbreviated / mixed-case)
APP_STATUS_ALIASES = {
    "completed": ["completed", "done", "success", "paid", "COMPLETED", "complete"],
    "cancelled":  ["cancelled", "cancel", "CANCEL", "CANCELLED"],
    "returned":   ["returned", "return", "refund", "REFUND"],
    "pending":    ["pending", "PENDING", "processing", "in_progress"],
    "unknown":    ["unknown", "other"],
}

# Status aliases for WEB (no CAPS abbreviations)
WEB_STATUS_ALIASES = {
    "completed": ["completed", "done", "success", "paid", "complete"],
    "cancelled":  ["cancelled", "cancel"],
    "returned":   ["returned", "return", "refund"],
    "pending":    ["pending", "processing", "in_progress"],
    "unknown":    ["unknown", "other"],
}

# Timezone options for WEB
WEB_TIMEZONES = ["+07:00"] * 50 + ["+08:00"] * 20 + ["Z"] * 30  # 50/20/30 weights


# ── Helpers ───────────────────────────────────────────────────────────────────

def random_datetime():
    """Random datetime between 2022-01-01 and 2024-12-31."""
    delta = END_DATE - START_DATE
    secs = random.randint(0, int(delta.total_seconds()))
    return START_DATE + timedelta(seconds=secs)


def trend_factor(dt):
    """1 + 0.1 * years_elapsed_since_2022-01-01."""
    years = (dt - START_DATE).total_seconds() / (365.25 * 24 * 3600)
    return 1 + 0.1 * years


def seasonal_factor(dt):
    m = dt.month
    if m in (11, 12):
        return 1.3
    elif m in (7, 8):
        return 1.1
    return 1.0


def random_total_amount(seller_type, dt):
    tf = trend_factor(dt)
    sf = seasonal_factor(dt)
    if seller_type == "high_performer":
        base = random.uniform(2000, 20000)
    elif seller_type == "average_seller":
        base = random.uniform(300, 3000)
    else:  # low_performer
        base = random.uniform(50, 500)
    return round(base * tf * sf, 2)


def pick_status(aliases_dict):
    """Pick a canonical status, then return one of its aliases at random."""
    canonical = random.choices(STATUS_VALUES, weights=STATUS_WEIGHTS, k=1)[0]
    return canonical, random.choice(aliases_dict[canonical])


def random_items(total_amount):
    """Generate 1-5 line items whose prices roughly sum to total_amount."""
    n = random.randint(1, 5)
    items = []
    remaining = total_amount
    for i in range(n):
        pid = random.choice(PRODUCTS)
        qty = random.randint(1, 4)
        if i == n - 1:
            unit_price = round(max(0.01, remaining / qty), 2)
        else:
            unit_price = round(random.uniform(remaining * 0.1, remaining * 0.6) / qty, 2)
            remaining -= unit_price * qty
            remaining = max(0.01, remaining)
        discount = round(random.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15]), 2)
        items.append({"pid": pid, "q": qty, "p": unit_price, "d": discount})
    return items


# ── App order generator ───────────────────────────────────────────────────────

def make_app_order(inject_error_flags):
    """
    App format (abbreviated JSON):
    {
      "oid": "uuid",
      "ts": unix_int,
      "slr": "S001",
      "st": "done",
      "usr": {"uid": "C000001", "rgn": "north"},
      "tot": 1500.00,
      "items": [{"pid":"P00001","q":2,"p":750.00,"d":0.05}]
    }
    """
    dt = random_datetime()
    sid = random.choice(SELLERS)
    stype = SELLER_TYPE[sid]
    rgn = SELLER_REGION[sid]
    uid = random.choice(CUSTOMERS)
    total = random_total_amount(stype, dt)
    canonical, alias_st = pick_status(APP_STATUS_ALIASES)
    items = random_items(total)

    record = {
        "oid": str(uuid.uuid4()),
        "ts": int(dt.timestamp()),
        "slr": sid,
        "st": alias_st,
        "usr": {"uid": uid, "rgn": rgn},
        "tot": total,
        "items": items,
    }

    # Inject errors
    null_usr, null_st, null_tot, neg_tot, outlier, null_pid = inject_error_flags

    if null_usr:
        record["usr"] = None
    if null_st:
        record["st"] = None
    if null_tot:
        record["tot"] = None
    elif neg_tot:
        record["tot"] = -abs(record["tot"]) if record["tot"] is not None else -1.0
    elif outlier:
        record["tot"] = round(total * 20, 2) if record["tot"] is not None else None

    if null_pid and record["items"]:
        record["items"][0]["pid"] = None

    return record


def generate_app_orders():
    """
    Generate N_APP orders with error injection:
    - null usr:      3%
    - null st:       2%
    - null tot:      4%
    - negative tot:  1%
    - outlier tot:   0.5%
    - null pid:      0.5%
    - _dup=True:     1% of orders (extra duplicate record injected)
    """
    orders = []
    dup_records = []

    for _ in range(N_APP):
        null_usr  = random.random() < 0.03
        null_st   = random.random() < 0.02
        null_tot  = random.random() < 0.04
        neg_tot   = (not null_tot) and (random.random() < 0.01)
        outlier   = (not null_tot) and (not neg_tot) and (random.random() < 0.005)
        null_pid  = random.random() < 0.005

        rec = make_app_order((null_usr, null_st, null_tot, neg_tot, outlier, null_pid))

        is_dup = random.random() < 0.01
        if is_dup:
            dup = dict(rec)
            dup["_dup"] = True
            dup_records.append(dup)

        orders.append(rec)

    # Insert duplicates at random positions
    for dup in dup_records:
        pos = random.randint(0, len(orders))
        orders.insert(pos, dup)

    return orders


# ── Web order generator ───────────────────────────────────────────────────────

def make_web_order(inject_error_flags):
    """
    Web format (flat JSON with metadata):
    {
      "order_id": "uuid",
      "order_date": "2022-01-15T08:30:00+07:00",
      "seller_id": "S001",
      "status": "completed",
      "customer_id": "C000001",
      "region": "north",
      "total_amount": 1500.00,
      "metadata": {"source": "web", "version": "2.1", "_retry": false}
    }
    """
    dt = random_datetime()
    sid = random.choice(SELLERS)
    stype = SELLER_TYPE[sid]
    rgn = SELLER_REGION[sid]
    uid = random.choice(CUSTOMERS)
    total = random_total_amount(stype, dt)
    canonical, alias_st = pick_status(WEB_STATUS_ALIASES)

    # ISO datetime with timezone
    tz_suffix = random.choice(WEB_TIMEZONES)
    order_date_str = dt.strftime("%Y-%m-%dT%H:%M:%S") + tz_suffix

    record = {
        "order_id":     str(uuid.uuid4()),
        "order_date":   order_date_str,
        "seller_id":    sid,
        "status":       alias_st,
        "customer_id":  uid,
        "region":       rgn,
        "total_amount": total,
        "metadata": {
            "source":  "web",
            "version": "2.1",
            "_retry":  False,
        },
    }

    # Inject errors
    null_cust, null_st, null_tot, neg_tot, outlier = inject_error_flags

    if null_cust:
        record["customer_id"] = None
    if null_st:
        record["status"] = None
    if null_tot:
        record["total_amount"] = None
    elif neg_tot:
        record["total_amount"] = -abs(record["total_amount"]) if record["total_amount"] is not None else -1.0
    elif outlier:
        record["total_amount"] = round(total * 20, 2) if record["total_amount"] is not None else None

    return record


def generate_web_orders():
    """
    Generate N_WEB orders with error injection:
    - null customer_id: 3%
    - null status:      2%
    - null total_amount:4%
    - negative total:   1%
    - outlier:          0.5%
    - _retry=True:      1% of orders (extra duplicate injected)
    """
    orders = []
    dup_records = []

    for _ in range(N_WEB):
        null_cust = random.random() < 0.03
        null_st   = random.random() < 0.02
        null_tot  = random.random() < 0.04
        neg_tot   = (not null_tot) and (random.random() < 0.01)
        outlier   = (not null_tot) and (not neg_tot) and (random.random() < 0.005)

        rec = make_web_order((null_cust, null_st, null_tot, neg_tot, outlier))

        is_dup = random.random() < 0.01
        if is_dup:
            dup = dict(rec)
            dup["metadata"] = dict(rec["metadata"])
            dup["metadata"]["_retry"] = True
            dup_records.append(dup)

        orders.append(rec)

    # Insert duplicates at random positions
    for dup in dup_records:
        pos = random.randint(0, len(orders))
        orders.insert(pos, dup)

    return orders


# ── File writing ──────────────────────────────────────────────────────────────

def write_part_files(records, local_dir, prefix, n_parts, part_size):
    """Split records into n_parts files of part_size each."""
    os.makedirs(local_dir, exist_ok=True)
    paths = []
    for part in range(n_parts):
        chunk = records[part * part_size: (part + 1) * part_size]
        fname = f"part-{part:05d}.jsonl"
        fpath = os.path.join(local_dir, fname)
        with open(fpath, "w") as f:
            for r in chunk:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"  Wrote {len(chunk):,} records → {fpath}")
        paths.append(fpath)
    return paths


def upload_to_hdfs(local_dir, hdfs_path):
    """Upload all files in local_dir to HDFS path."""
    # Create HDFS directory
    r = subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_path],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"  [WARN] mkdir -p {hdfs_path}: {r.stderr.strip()}")

    # Upload each file
    for fname in sorted(os.listdir(local_dir)):
        local_file = os.path.join(local_dir, fname)
        hdfs_target = f"{hdfs_path}/{fname}"

        # Remove existing file first
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-f", hdfs_target],
            capture_output=True
        )

        r = subprocess.run(
            ["hdfs", "dfs", "-put", local_file, hdfs_path],
            capture_output=True, text=True
        )
        if r.returncode == 0:
            print(f"  Uploaded → {hdfs_target}")
        else:
            print(f"  [ERROR] Upload failed for {local_file}: {r.stderr.strip()}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("STEP 0 — Generate Orders (200k)")
    print("=" * 60)
    t0 = time.time()

    # ── APP orders ────────────────────────────────────────────────────────────
    print(f"\n[APP] Generating {N_APP:,} app orders …")
    t1 = time.time()
    app_orders = generate_app_orders()
    print(f"  Done in {time.time()-t1:.1f}s  (total with dups: {len(app_orders):,})")

    local_app_dir = "/tmp/sales_app"
    print(f"[APP] Writing part files to {local_app_dir} …")
    write_part_files(app_orders, local_app_dir, "app", n_parts=2, part_size=60_000)

    hdfs_app = f"{HDFS}/sales/raw/source=app"
    print(f"[APP] Uploading to HDFS {hdfs_app} …")
    upload_to_hdfs(local_app_dir, hdfs_app)

    # ── WEB orders ────────────────────────────────────────────────────────────
    print(f"\n[WEB] Generating {N_WEB:,} web orders …")
    t2 = time.time()
    web_orders = generate_web_orders()
    print(f"  Done in {time.time()-t2:.1f}s  (total with dups: {len(web_orders):,})")

    local_web_dir = "/tmp/sales_web"
    print(f"[WEB] Writing part files to {local_web_dir} …")
    write_part_files(web_orders, local_web_dir, "web", n_parts=2, part_size=40_000)

    hdfs_web = f"{HDFS}/sales/raw/source=web"
    print(f"[WEB] Uploading to HDFS {hdfs_web} …")
    upload_to_hdfs(local_web_dir, hdfs_web)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print(f"STEP 0 DONE — {elapsed:.1f}s")
    print(f"  Base orders  : {N_APP + N_WEB:,}")
    print(f"  App with dups: {len(app_orders):,}")
    print(f"  Web with dups: {len(web_orders):,}")
    print("  HDFS paths:")
    print(f"    {hdfs_app}/part-00000.jsonl  (60k records)")
    print(f"    {hdfs_app}/part-00001.jsonl  (60k records)")
    print(f"    {hdfs_web}/part-00000.jsonl  (40k records)")
    print(f"    {hdfs_web}/part-00001.jsonl  (40k records)")
    print("=" * 60)


if __name__ == "__main__":
    main()
